# Uninstalling The JMS Bridge
To remove the JMS-Bridge itself from Linux execute on terminal:

1. Stop installed broker running via 
    '''console
    kill -9 <pid>
    '''
     
2. 'Optional:' Remove / Delete the JMS Broker Directory
    '''console
    rm -rf /jms-bridge-server-0.1.0-SNAPSHOT/*
    '''