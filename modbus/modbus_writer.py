import json
import logging.config
import os
import time
import threading

import docker
import modbus_server
import redis

from logger.setup_logging import setup_logging


# Configuration
MODBUS_HOST = os.getenv("MODBUS_HOST", "0.0.0.0")
MODBUS_PORT = int(os.getenv("MODBUS_PORT", 502))

MY_CONVERTER = os.getenv("MODBUS_SERVICE_CONVERTER", "converter")
MY_REDIS = os.getenv("MODBUS_SERVICE_REDIS", "redis")
MY_WRITER = os.getenv("MODBUS_SERVICE_WRITER", "modbus")
HEALTH_MODBUS_TOGGLE = os.getenv("HEALTH_MODBUS_TOGGLE", "health:modbus_toggle")
MY_UPLOADER = os.getenv("MODBUS_SERVICE_UPLOADER", "uploader")
MY_COMBINER = os.getenv("MODBUS_SERVICE_COMBINER", "combiner")
MY_FETCHER = os.getenv("MODBUS_SERVICE_FETCHER", "fetcher")
HEALTH_KEY_ALLSAT = os.getenv("HEALTH_KEY_ALLSAT", "health:allsat_fetch")
HEALTH_UDBF_FILE_SIZE = os.getenv("HEALTH_UDBF_FILE_SIZE", "health:udbf_file_size")

logger = logging.getLogger("modbus")

# Healthcheck steup
docker_client = docker.DockerClient(
    base_url='unix:///var/run/docker.sock',
    timeout=3
)
prev = {}

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
            "starting": -1,
            "healthy":   0,
            "unhealthy": 1
        }
    modbus_map = {
            "starting": -1,
            "healthy":   1,
            "unhealthy": 0
        }
    
    # Load mapping.json
    mapping_path = os.getenv("MAPPING_PATH", "setup/mapping.json")
    logger.debug(f"Loading mapping from {mapping_path}")
    with open(mapping_path) as f:
        MAPPINGS = json.load(f)

    # Start Modbus server
    try:
        server = modbus_server.Server(host=MODBUS_HOST, port=MODBUS_PORT)
        server.start()
        logger.debug(f"Modbus server started on {MODBUS_HOST}:{MODBUS_PORT}")
        """# Prefill holding registers to avoid "Illegal Data Address" errors
        highest_register = max(entry["register"] for entry in MAPPINGS) + 1  # +1 to cover float (2 registers)
        for addr in range(0, highest_register + 1, 2):  # float32 needs 2 registers
            server.set_holding_register(addr, 0.0, "f")
        logger.debug(f"Prefilled holding registers 0 to {highest_register}")"""
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
        logger.info(f"Heartbeat to {int(heartbeat_state)} at register {REG_HEARTBEAT}")
        t = threading.Timer(10*60, flip_heartbeat)
        t.daemon = True
        t.start()

    flip_heartbeat()

    last_key = None

    # Writer loop
    logger.debug(f"Starting Redis to Modbus loop...")
    try:
        while True:
            keys = redis_db.keys("stats:*")
            if not keys:
                # No data yet.
                time.sleep(1)
                continue

            stats_key = sorted(keys)[-1]
            if stats_key == last_key:
                time.sleep(1)
                continue
            logger.debug(f"Using Redis key: {stats_key}")
            last_key = stats_key

            # Write all sensor values.
            for entry in MAPPINGS:
                field    = entry["field"]
                register = entry["register"]
                val       = redis_db.hget(stats_key, field)

                if val is None:
                    continue

                try:
                    float_val = float(val.replace(",", "."))
                except ValueError:
                    logger.warning(f"Cannot parse {val!r} for field '{field}'")
                    continue

                server.set_holding_register(register, float_val, "f")
                logger.debug(f"Wrote {float_val} for field {field} to register {register} and {register +1}")
       
            for svc_name, reg in [
                (MY_FETCHER,  110)
            ]:
                try:
                    ctr = docker_client.containers.get(svc_name)
                    health = ctr.attrs["State"].get("Health", {}).get("Status", "unhealthy")
                except Exception:
                    health = "unhealthy"

                mapping = modbus_map if svc_name == MY_WRITER else default_map
                code = mapping.get(health, mapping["unhealthy"])

                # Only rewrite on actual change:
                if prev.get((svc_name, reg)) != code:
                    logger.debug(f"{svc_name}: {health!r} mapped to code {code} in register {reg}")
                    server.set_holding_register(reg, float(code), "f")
                    prev[(svc_name, reg)] = code

            for env_key, reg_addr in [
                (HEALTH_UDBF_FILE_SIZE, 124)
            ]:
                redis_key = env_key
                if not redis_key:
                    logger.warning(f"Skiped health loop for env_key={env_key!r}, got empty redis_key")
                    continue

                try:
                    val = redis_db.get(redis_key)
                    code = int(val) if val is not None else 1
                except Exception:
                    code = 1

                if prev.get((redis_key, reg_addr)) != code:
                    server.set_holding_register(reg_addr, code, "H")
                    logger.debug(f"Wrote {redis_key} with {code} to address {reg_addr}.")
                    prev[(redis_key, reg_addr)] = code

            time.sleep(1)

    except Exception:
        logger.exception("Error in modbus writer loop")

if __name__ == "__main__":
    main()
