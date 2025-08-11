from ginsapy.giutility.connect.PyQStationConnectWin import ConnectGIns


class GInsConnection(ConnectGIns):
    """
    Serves to create a context manager to create and close a connection to Gantner Controller.
    """

    def __enter__(self) -> 'GInsConnection':
        return self
    
    def __exit__(self, exc_type: type[BaseException] | None,
                  exc_value: BaseException | None,
                  traceback: object | None) -> None:
        self.close_connection()