Alarmvariablen einer Pipeline:
- file size -> Prüft ob die Dateigröße zu stark vom Richtwert abweicht. Wird mit jeder eingelesen Datei gesetzt, 0 für healthy, 1 für unhealthy
- file processing -> Wird auf 1 gesetzt, wenn es irgendwelche Probleme beim verarbeiten einer Datei gibt
- container health -> Heartbeat thread, setzt container healthcheck 
- file ingestion ->

Aktuelle health vars:
- health:lpi_100hz_file_size
- health:container_conv_lpi
- health:lpi_100hz_file_processing
- health:lpi_100hz_file_ingestion

Future improvements:
- The LPI converter pipeline currently runs in a separate container 
image due to instability in Gantner’s .so libraries on amd64. This
forces emulation and results in significantly longer, inconsistent 
build times. Action: once a stable amd64-compatible .so is available, 
merge the LPI pipeline into the main converter image.
