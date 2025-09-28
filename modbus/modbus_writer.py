import json
import logging
import os
import time
import threading

import docker
import modbus_server
import redis


from logger.setup_logging import setup_logging

MODBUS_HOST = os.getenv("MODBUS_HOST", "0.0.0.0")
MODBUS_PORT = int(os.getenv("MODBUS_PORT", 502))

MY_CONV_LPI = os.getenv("MODBUS_SERVICE_CONV_LPI", "conv_lpi")
MY_REDIS = os.getenv("MODBUS_SERVICE_REDIS", "redis")
HEALTH_MODBUS_TOGGLE = os.getenv("HEALTH_MODBUS_TOGGLE", "health:modbus_toggle")

logger = logging.getLogger("modbus")

# Healthcheck steup
docker_client = docker.DockerClient(
    base_url='unix:///var/run/docker.sock',
    timeout=3
)

FLOAT_EPS = 1e-9
last_written = {} 

def write_if_changed(server, reg: int, value: float):
    prev = last_written.get(reg)
    if prev is None or abs(prev - value) > FLOAT_EPS:
        server.set_holding_register(reg, float(value), "f")
        last_written[reg] = float(value)

def main():
    setup_logging(process_name="modbus")

    redis_db = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        decode_responses=True
    )

    # Mapping for the healthchecks
    default_map = {
            "starting": -1.0,
            "healthy":   0.0,
            "unhealthy": 1.0
        }
    
    mapping_path = os.getenv("MAPPING_PATH", "setup/mapping.json")
    logger.debug(f"Loading mapping from {mapping_path}.")
    with open(mapping_path) as f:
        MAPPINGS = json.load(f)

    try:
        server = modbus_server.Server(host=MODBUS_HOST, port=MODBUS_PORT)
        server.start()
        logger.debug(f"Modbus server started on {MODBUS_HOST}:{MODBUS_PORT}")
        highest_register = max(entry["register"] for entry in MAPPINGS) + 1  # +1 to cover float (2 registers)
        for addr in range(0, highest_register + 1, 2):  
            server.set_holding_register(addr, 0.0, "f")
            last_written[addr] = 0.0
        logger.debug(f"Prefilled holding registers 0 to {highest_register}.")
    except Exception:
        logger.exception(f"Failed to start Modbus server.")
        exit(1)

    # Flip Heartbeat
    REG_HEARTBEAT = 120 
    heartbeat_state = False

    def flip_heartbeat():
        nonlocal heartbeat_state
        heartbeat_state = not heartbeat_state
        server.set_holding_register(REG_HEARTBEAT, float(heartbeat_state), "f")
        logger.info(f"Heartbeat to {int(heartbeat_state)} at register {REG_HEARTBEAT}.")
        t = threading.Timer(10*60, flip_heartbeat)
        t.daemon = True
        t.start()

    flip_heartbeat()


    # writer loop
    logger.debug("Starting Redis→Modbus  writer loop...")
    HEALTH_POLL_SEC = 3.0
    next_health_poll = 0.0

    try:
        while True:
            # stat values
            for stats_key in redis_db.scan_iter("stats:*"):
                logger.debug(f"Using redis key: {stats_key}")
                for entry in MAPPINGS:
                    field = entry.get("field")
                    if not field:
                        continue
                    register = entry["register"]

                    val = redis_db.hget(stats_key, field)
                    if val is None:
                        continue

                    try:
                        float_val = float(str(val).strip().replace(",", "."))
                    except ValueError:
                        logger.warning(f"Cannot parse {val!r} for field '{field}' from {stats_key}.")
                        continue

                    write_if_changed(server, register, float_val)
                    # logger.debug(f"Wrote {float_val} from {stats_key}.{field} → HR {register}")
            
            # health values
            for stats_key in redis_db.scan_iter("health:*"):
                logger.debug(f"Using redis key: {stats_key}")
                for entry in MAPPINGS:
                    field = entry.get("field")
                    if not field:
                        continue
                    register = entry["register"]

                    val = redis_db.hget(stats_key, field)
                    if val is None:
                        continue

                    try:
                        float_val = float(str(val).strip().replace(",", "."))
                    except ValueError:
                        logger.warning(f"Cannot parse {val!r} for field '{field}' from {stats_key}.")
                        continue

                    write_if_changed(server, register, float_val)
                    # logger.debug(f"Wrote {float_val} from {stats_key}.{field} → HR {register}")

            # Container Health
            now = time.monotonic()
            
            if now >= next_health_poll:
                for svc_name, reg in [
                    (MY_CONV_LPI, 100),
                    (MY_REDIS, 101),
                ]:
                    try:
                        lst = docker_client.containers.list(all=True, filters={"label": f"com.docker.compose.service={svc_name}"})
                        ctr = lst[0] if lst else docker_client.containers.get(svc_name)
                        attrs = ctr.attrs or {}
                        state = attrs.get("State") or {}
                        health = (state.get("Health") or {}).get("Status")
                        if health is None:
                            health = "healthy" if state.get("Running") else "unhealthy"
                    except Exception:
                        health = "unhealthy"

                    code = default_map.get(health, default_map["unhealthy"])
                    write_if_changed(server, reg, code)
            
                next_health_poll = now + HEALTH_POLL_SEC

            time.sleep(0.2)

    except Exception:
        logger.exception("Error in modbus writer loop.")

if __name__ == "__main__":
    main()
