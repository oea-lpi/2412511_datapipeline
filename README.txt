Future improvements:
- The LPI converter pipeline currently runs in a separate container 
image due to instability in Gantner’s .so libraries on amd64. This
forces emulation and results in significantly longer, inconsistent 
build times. Action: once a stable amd64-compatible .so is available, 
merge the LPI pipeline into the main converter image.
