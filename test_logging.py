import logging
LOG_NAME = 'log.txt'
FORMAT = '%(asctime)s  - %(levelname)s - %(message)s'
logging.basicConfig(filename=LOG_NAME,level=logging.INFO,format=FORMAT)
logger = logging.getLogger(__name__)
logger.info('asdfsadf He'.encode('utf8'))

# FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
# logging.basicConfig(format=FORMAT)
# d = {'clientip': '192.168.0.1', 'user': 'fbloggs'}
# logger = logging.getLogger('tcpserver')
# logger.warning('Protocol problem: %s', 'connection reset', extra=d)