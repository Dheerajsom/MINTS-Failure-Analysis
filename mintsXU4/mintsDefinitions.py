from getmac import get_mac_address
import serial.tools.list_ports
import yaml

def findPort(find):
    ports = list(serial.tools.list_ports.comports())
    for p in ports:
        currentPort = str(p)
        if(currentPort.endswith(find)):
            return(currentPort.split(" ")[0])

def findIPSPorts():
    ports = list(serial.tools.list_ports.comports())
    ipsPorts = []
    for p in ports:
        currentPort = str(p[1])
        if(currentPort.find("CBL-7100")>=0):
            ipsPorts.append(str(p[0]).split(" ")[0])
    return ipsPorts
  
def findAirmarPort():
    ports = list(serial.tools.list_ports.comports())
    ozonePort = []
    for p in ports:
        currentPort = str(p[2])
        if(currentPort.find("PID=10C4:EA60 SER=0001")>=0):
            ozonePort.append(str(p[0]).split(" ")[0])
    return ozonePort

def findUSBRG15Port():
    ports = list(serial.tools.list_ports.comports())
    ozonePort = []
    for p in ports:
        currentPort = str(p[1])
        if(currentPort.find("CP2102 USB to UART Bridge Controller")>=0):
            ozonePort.append(str(p[0]).split(" ")[0])
    return ozonePort


def findUSBGPSPort():
    ports = list(serial.tools.list_ports.comports())
    ozonePort = []
    for p in ports:
        print(p.device, p.description)
        description = str(p.description)
        if "u-blox 7 - GPS/GNSS Receiver" in description:
            return p.device  
    return ozonePort


def findMacAddress():
    macAddress= get_mac_address(interface="eth0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="docker0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="enp1s0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    macAddress= get_mac_address(interface="wlan0")
    if (macAddress!= None):
        return macAddress.replace(":","")

    return "xxxxxxxx"

fPortIDs                  = yaml.load(open('mintsXU4/credentials/portIDs.yml'),Loader=yaml.FullLoader)['portIDs']
mintsDefinitions         = yaml.load(open('mintsXU4/credentials/mintsDefinitions.yaml'),Loader=yaml.FullLoader)

dataFolderReference       = "/home/teamlary/mintsData/reference"
dataFolderMQTTReference   = "/home/teamlary/mintsData/referenceMQTT"
dataFolder                = "/home/teamlary/mintsData/raw"
dataFolderMQTT            = "/home/teamlary/mintsData/rawMQTT"
dataFolderTmp             = "/home/teamlary/mintsDataTmp"
# dataFolderJson            = mintsDefinitions['dataFolderJson']

ipsPorts              = findIPSPorts()
USBRG15Port           = findUSBRG15Port()
USBGPSPort            = findUSBGPSPort()

macAddress            = findMacAddress()
latestDisplayOn       = False
latestOn              = False
airmarPort            = findAirmarPort()

# For MQTT 
mqttOn                = True
mqttCredentialsFile   = 'mintsXU4/credentials.yml'
mqttBroker            = "mqtt.circ.utdallas.edu"
mqttPort              =  8883  # Secure port
gpsPort               = findPort("GPS/GNSS Receiver")


# For Humidity Corrections
pmSensor      = "IPS7100"
climateSensor = "BME280V2"
modelFile     = 'mintsXU4/climateCorrectionModel.joblib'


if __name__ == "__main__":
    # the following code is for debugging
    # to make sure everything is working run python3 mintsDefinitions.py 
    print("Mac Address                : {0}".format(macAddress))
    print("Data Folder Reference      : {0}".format(dataFolderReference))
    print("Data Folder Raw            : {0}".format(dataFolder))
    print("Latest On                  : {0}".format(latestOn))
    print("MQTT On                    : {0}".format(mqttOn))
    print("MQTT Credentials File      : {0}".format(mqttCredentialsFile))
    print("MQTT Broker and Port       : {0}, {1}".format(mqttOn,mqttPort))
    #-------------------------------------------#
    print("IPS Ports :")
    for dev in ipsPorts:
        print("\t{0}".format(dev))
        
    print("USB RG15 Port              : {0}".format(USBRG15Port))        
    print("USB GPS Port               : {0}".format(USBGPSPort))        