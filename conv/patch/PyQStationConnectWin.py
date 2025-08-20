#-*- coding: utf-8 -*-

"""
Created on Mon Jun 29 11:09:57 2020
@author: BJ
Gantner Instruments API Python interface - Examples
This module contains the python communication functions
"""

from ctypes import*
import ctypes
import numpy as np
import os


class ConnectGIns():
    """This is the connection class to Gantner Controller 
	Parameters was given standard values during initialisation. Some changes 
    could be needed in some application"""
    def __init__(self):
			# Load DLL into memory, take care to have the corresponding dll to the computer operating system
        try:
            path_dir = os.path.dirname(__file__)          
            path_parent = os.path.dirname(path_dir)
            path_dll = os.path.join(path_parent, 'libGInsUtility_x64.so') #Linux version, for windows substitute with giutility_x64.dll
            self.GINSDll = ctypes.cdll.LoadLibrary(path_dll)
            print("64 bit DLL imported")
        except OSError:
            path_dir = os.path.dirname(__file__)          
            path_parent = os.path.dirname(path_dir)
            path_dll = os.path.join(path_parent, 'libGInsUtility_x86.so')
            self.GINSDll = ctypes.cdll.LoadLibrary(path_dll)
            print("32 bit DLL imported")

		#if linux uncomment before and use
		#self.GINSDll = ctypes.cdll.LoadLibrary("PyQStationConnect_2_0\\libGInsData.so")
		#print("Linux System")
        #function prototypes
        self.GINSDll._CD_eGateHighSpeedPort_Init.argtypes = [c_char_p,c_int,c_int,c_int,POINTER(c_int),POINTER(c_int)]
        self.GINSDll._CD_eGateHighSpeedPort_SetBackTime.argtypes = [c_int, c_double]
        self.GINSDll._CD_eGateHighSpeedPort_InitBuffer.argtypes=[c_int,c_int,c_int]
        self.GINSDll._CD_eGateHighSpeedPort_DecodeFile_Select.argtypes = [POINTER(c_int), POINTER(c_int), c_char_p]
        self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo.argtypes = [c_int, c_int, c_int, POINTER(c_double), c_char_p]
        self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_String.argtypes = [c_int, c_int, c_int,c_int,c_char_p]
        self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_Int.argtypes = [c_int, c_int, c_int,c_int,POINTER(c_int)]
        self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray.argtypes = [c_int, POINTER(c_double), c_int, c_int,POINTER(c_int), POINTER(c_int), POINTER(c_int)]
        self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single_Immediate.argtypes = [c_int, c_int, c_double]
        self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single.argtypes = [c_int, c_int, c_double]
        self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_ReleaseOutputData.argtypes = [c_int]
        self.GINSDll._CD_eGateHighSpeedPort_Close.argtypes = [c_int, c_int]        


        #self.GINSDll._CD_eGateHighSpeedPort_GetPostProcessBufferInfo.argtypes = [c_int,c_char_p,c_size_t,c_char_p,c_size_t]   
        self.GINSDll._CD_eGateHighSpeedPort_Init_PostProcessBuffer.argtypes = [c_char_p,POINTER(c_int),POINTER(c_int)]
        #self.bufferID=ctypes.create_string_buffer(50)
        #self.bufferName=ctypes.create_string_buffer(50)
        
        """"  parameters for Init connection """
        self.controllerIP=0#controllerIP.encode('UTF-8')
        self.timeout=5
        self.HSP_BUFFER=2#  1 for online; 2 for buffered values
        self.HSP_ONLINE=1#  1 for online; 2 for buffered values
        self.sampleRate=100
        # parameters for file decoding
        self.FilePath=0# = FilePath.encode('UTF-8')
        self.FileDecodeComplete=False
        #general used parameters
        self.HCLIENT=c_int(-1)
        self.HCONNECTION=c_int(-1)
        #parameters for Init buffer
        self.bufferindex=0
        self.autoRun=0
        #parameters to empty the circular buffer
        self.backtime=0
        #parameters to read information from devices
        self.location=10
        self.Adress=11
        self.SampleRate=16
        self.SerialNumber=15
        self.ChannelCount=18
        self.Channel_InfoName=0
        self.Channel_Unit=1
        self.info=c_double(0)
        self.ret=0
        self.char=ctypes.create_string_buffer(30)
        self.CHINFO_INDX=7#total index
        self.DADI_INOUT=2#input/output
        self.ChannelInfo=c_int(-1)
        self.channelInfoStr=ctypes.create_string_buffer(30)
        #parameters to read buffer
        
        
    def init_connection(self,controllerIP):
        self.controllerIP=controllerIP.encode('UTF-8')
        """Initialisation of the connection to a controller - buffer connection 
		self.GINSDll._CD_eGateHighSpeedPort_Init(self.controllerIP,self.timeout, 
        self.HSP_BUFFER,self.sampleRate,byref(self.HCLIENT),byref(self.HCONNECTION))"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_Init(self.controllerIP,self.timeout,self.HSP_BUFFER,self.sampleRate,byref(self.HCLIENT),
        byref(self.HCONNECTION))
        if(ret!=0):
            print("Init Connection Failed - ret:",ret)
            self.ret=ret
            return False

        #Init buffer (this is mainly to select a certain buffer by index)
        ret=self.GINSDll._CD_eGateHighSpeedPort_InitBuffer(self.HCONNECTION.value,self.bufferindex,self.autoRun)
        if(ret!=0):
            print("Init Buffer Failed - ret:",ret)
            return False
        #empty the circular buffer to get only actual data
        ret=self.GINSDll._CD_eGateHighSpeedPort_SetBackTime(self.HCONNECTION.value,self.backtime)
        if(ret!=0):
            print("SetBackTime Failed - ret:",ret)
            return False
        print("Connection initialized. IP: ", controllerIP)
        return True
    
    
    def init_online_connection(self,controllerIP):
        self.controllerIP=controllerIP.encode('UTF-8')
        """Initialisation of the connection to a controller - online connection 
		self.GINSDll._CD_eGateHighSpeedPort_Init(self.controllerIP,self.timeout, 
        self.HSP_ONLINE,self.sampleRate,byref(self.HCLIENT),byref(self.HCONNECTION))"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_Init(self.controllerIP,self.timeout,self.HSP_ONLINE,self.sampleRate,byref(self.HCLIENT),byref(self.HCONNECTION))
        if(ret!=0):
            print("Init Connection Failed - ret:",ret)
            self.ret=ret
            return False

        #Init buffer (this is mainly to select a certain buffer by index)
        ret=self.GINSDll._CD_eGateHighSpeedPort_InitBuffer(self.HCONNECTION.value,self.bufferindex,self.autoRun)
        if(ret!=0):
            print("Init Buffer Failed - ret:",ret)
            return False
        #empty the circular buffer to get only actual data
        ret=self.GINSDll._CD_eGateHighSpeedPort_SetBackTime(self.HCONNECTION.value,self.backtime)
        if(ret!=0):
            print("SetBackTime Failed - ret:",ret)
            return False
        print("Connection initialized. IP: ", controllerIP)
        return True
    
    
    def init_file(self,FilePath):
        """Initialisation of the dat-file decoding 
		self.GINSDll._CD_eGateHighSpeedPort_DecodeFile_Select(byref(self.HCLIENT), 
        byref(self.HCONNECTION),self.FilePath)"""
        self.FilePath= FilePath.encode('UTF-8')
        ret=self.GINSDll._CD_eGateHighSpeedPort_DecodeFile_Select(byref(self.HCLIENT),byref(self.HCONNECTION),self.FilePath)
        if ret==0:
            print("File Load OK!", self.FilePath.decode('UTF-8'))
            return True
        else:
            print("Error Loading File ", self.FilePath.decode('UTF-8'))
            return False

    def read_serial_number(self):
        """Read the serial number of the connected device 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.SerialNumber,0,self.info,None)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.SerialNumber,0,self.info,None)
        if(ret==0):
            print("controller serial number", self.info.value)
            return self.info.value
        else:
            print("error reading serial number!")
            return 0
                
    def read_sample_rate(self):
        """Read a buffer sampling rate
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.SampleRate,0,self.info,None)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.SampleRate,0,self.info,None)
        if(ret==0):
            print("controller sample rate", self.info.value)
            return self.info.value
        else:
            print("Error reading sample rate!")
            return 0
        
    def read_channel_count(self):
        """Count the number of channels connected to a controller 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.ChannelCount,0,self.info,None)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.ChannelCount,0,self.info,None)
        if(ret==0):
            print("controller channel count", self.info.value)
            return self.info.value
        else:
            print("Error reading channel count!")
            return ""
    
    def read_controller_name(self):
        """Read a controller name 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.location,0,None,self.char)"""
        #p=ctypes.create_string_buffer(30)#this function works for python 3.6, for lower version the name of functions to create mutable character buffer is different
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.location,0,None,self.char)
        if(ret==0):
            print("controller name", self.char.value.decode('UTF-8'))
            return self.char.value.decode('UTF-8')
        else:
            print("Error reading controller name!")
            return ""
        
    def read_controller_address(self):
        """Read the adress of a connected controller 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.Adress,0,None,self.char)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.Adress,0,None,self.char)
        if(ret==0):
            print("controller adress", self.char.value.decode('UTF-8'))
            return self.char.value.decode('UTF-8')
        else:
            print("Error reading controller address")
            return ""
        
    def read_channel_names(self):
        """Read the channel name and corresponding index 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.ChannelCount,0,self.info,None)"""
        i=0
        self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.ChannelCount,0,self.info,None)
        ChannelNb=self.info.value
        while i<ChannelNb:
            self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.Channel_InfoName,i,None,self.char)
            print("Controller index:",i," channel name:", self.char.value.decode('UTF-8'))
            i+=1
    def read_channels_unit(self):
        """Read the channel unit 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.ChannelCount,0,self.info,None)"""
        i=0
        self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.ChannelCount,0,self.info,None)
        ChannelNb=self.info.value
        while i<ChannelNb:
            self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.Channel_Unit,i,None,self.char)
            try:
                print("Controller index:",i," channel unit:", self.char.value.decode('UTF-8'))
            except UnicodeDecodeError:
                print("Controller index:",i," channel unit:", "NAN")
            i+=1        
         
    def read_index_unit(self,IndexNb):
        """Read the channel name corresponding to an index 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.Channel_Unit,IndexNb,None,self.char)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.Channel_Unit,IndexNb,None,self.char)
        if(ret==0):
            try:
                return(self.char.value.decode('UTF-8'))
            except UnicodeDecodeError:
                # we decode °C
                if self.char.value == b'\xb0C':
                    return (u'\u00b0C')
                # we decode
                if self.char.value == b'\xb5m/m':
                    return (u'\u00b5m/m')
                #we decode
                
                else:
                    return("Unit")
        else:
            print("Error reading channel unit, index",IndexNb)
            return ""
           
    def read_index_name(self,IndexNb):
        """Read the channel name corresponding to an index 
		self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value, 
        self.Channel_InfoName,IndexNb,None,self.char)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.Channel_InfoName,IndexNb,None,self.char)
        if ret != 0:
            print("Error reading channel name, index", IndexNb)
            return ""

        data = self.char.value
        # first try UTF-8
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            # fallback to Latin-1, replacing any invalid bytes
            return data.decode("latin-1", errors="replace")
        
    def write_online_value(self,IndexNb,WriteValue):
        """Write a single double value to a specific channel on the connection, 
        selected with connectionIndex immeadiately. 
		self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single_Immediate(self.HCONNECTION.value,IndexNb,WriteValue)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single_Immediate(self.HCONNECTION.value,IndexNb,WriteValue)
        if(ret==0):
            print("Value:",WriteValue,"Was added to index:",IndexNb)
            return True
        else:
            print("Could not writte value:",WriteValue,"to index:",IndexNb)
            return False
 
    
    def write_single(self,IndexNb,WriteValue):
        """Writte a single double value to a specific channel selected 
        with connectionIndex. 
        They will be stored in the DLL output buffer 
        until "eGateHighSpeedPort_WriteOnline_ReleaseOutputData" is called for this connection. 
		self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single(self.HCONNECTION.value,IndexNb,WriteValue)"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_Single(self.HCONNECTION.value,IndexNb,WriteValue)
        if(ret==0):
            print("Value",WriteValue,"was written")
            return ""
        else:
            print("Error writting channel value, index")
            return ""
    def relase_output(self):
       """Release all bufered output values. This ensures that all channels are written simultaniously. 
	   self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_ReleaseOutputData(self.HCONNECTION.value)"""
       ret=self.GINSDll._CD_eGateHighSpeedPort_WriteOnline_ReleaseOutputData(self.HCONNECTION.value)
       if(ret==0):
           print("Value was released")
           return ""
       else:
           print("Error releasing data")
           return ""   
    
    #def read_buffer_frame(self):
    #    self.GINSDll._CD_eGateHighSpeedPort_GetBufferFrames.argtypes=[c_int,c_int]
    #    self.GINSDll._CD_eGateHighSpeedPort_GetBufferFrames(self.HCONNECTION.value,self.HCLIENT.value)
    #    print("buffer frame number:",)
        
    
    def yield_buffer(self,NbFrames=int(100000),fillArray=0):
        '''function used to received the buffer content - the stream is considered 
        like a generator 
        self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray(self.HCONNECTION.value, 
        valuesPtr,(NbFrames*ChannelNb),fillArray,ReceivedFrames,ReceivedChannels,ReceivedComplete)'''

        self.GINSDll._CD_eGateHighSpeedPort_GetDeviceInfo(self.HCONNECTION.value,self.ChannelCount,0,self.info,None)
        ChannelNb=int(self.info.value)
        valuesPtr=(c_double*(NbFrames*ChannelNb))()
        ReceivedFrames=c_int(0)#pointer
        ReceivedChannels=c_int(0)#pointer
        ReceivedComplete=c_int(0)#pointer
        ret=0
        while(ret==0):
            ret=self.GINSDll._CD_eGateHighSpeedPort_ReadBufferToDoubleArray(self.HCONNECTION.value,valuesPtr,(NbFrames*ChannelNb),fillArray,ReceivedFrames,ReceivedChannels,ReceivedComplete)
            chcnt=ReceivedChannels.value
            BUF=valuesPtr[0:chcnt*ReceivedFrames.value]
            buffer=np.reshape(BUF,(ReceivedFrames.value,chcnt))
            yield buffer


    def close_connection(self):
        '''close the connection
        self.GINSDll._CD_eGateHighSpeedPort_Close(self.HCONNECTION.value,self.HCLIENT.value)'''
        self.GINSDll._CD_eGateHighSpeedPort_Close(self.HCONNECTION.value,self.HCLIENT.value)


    def init_buffer_conn(self,sbufferID):
        '''Initialize connection to PostProcess buffer 
        self.GINSDll._CD_eGateHighSpeedPort_Init_PostProcessBuffer(self.sbufferID, 
        byref(self.HCLIENT),byref(self.HCONNECTION))'''
        self.sbufferID=sbufferID.encode('UTF-8')
        ret=self.GINSDll._CD_eGateHighSpeedPort_Init_PostProcessBuffer(self.sbufferID,byref(self.HCLIENT),byref(self.HCONNECTION))
        if(ret!=0):
            print("Init Connection Failed - ret:",ret)

    def get_channel_info(self,IndexNb):
        """Read the channel total index"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_Int(self.HCONNECTION.value,self.CHINFO_INDX,self.DADI_INOUT,IndexNb,byref(self.ChannelInfo))
        if(ret==0):
            print("Total index of selected channel", self.ChannelInfo.value)
            return self.ChannelInfo.value
        else:
            print("Error reading channel info!")
            return ""

    def get_channel_info_name(self,IndexNb):
        """Read the channel name corresponding to the upper index"""
        ret=self.GINSDll._CD_eGateHighSpeedPort_GetChannelInfo_String(self.HCONNECTION.value,0,self.DADI_INOUT,IndexNb,self.channelInfoStr)
        if(ret==0):
            return self.channelInfoStr.value.decode('UTF-8')
        else:
            print("Error reading channel info!")
            return ""

#This function is needeed for file saved with higu number of frames
def read_again_buffer(buffer):
    '''help function called in read_gins_dat'''
    read_next_buffer=next(buffer)
    new_buffer_values= read_next_buffer[:,:]
    return (new_buffer_values)


def read_gins_dat(connection):   
    '''function calles to read dat file and ensure than all frames are readed. 
    Indeed the number of frames is not known in the header of udbf file, a check 
    of the timestampl is done in this loop''' 
    #Call function to store buffer into the variable buffer
    buffer=connection.yield_buffer()
    #We read the dat file and save it into the variable disp
    readbuffer=next(buffer) 
    Logged_file = readbuffer[:,:]        
    #This function is needed for reading the full dat file with several buffer frames    
    while True:
        #print('we enter while')
        new_disp=read_again_buffer(buffer)
        try:
            new_time=new_disp[0,0]
        except:
            break
        last_time=Logged_file[-1,0]
        if new_time>last_time:
            Logged_file=np.vstack((Logged_file,new_disp))
        else:
            break
    print ('dat file was read')
    return Logged_file
	
def create_list_channel(connection):
    '''Go through the buffer and return some list of channels names or unit'''
    Nb=0
    list_channels=[]
    list_unit=[]
    while Nb<int(connection.read_channel_count()):
        try:
            #list_channels.append(connection.read_index_name(Nb).encode('UTF-8'))
            list_channels.append(str(connection.read_index_name(Nb)))
        except AttributeError:
            list_channels.append(str(connection.read_index_name(Nb)))
        list_unit.append(connection.read_index_unit(Nb).lstrip())      
        Nb+=1
    return (list_channels,list_unit)