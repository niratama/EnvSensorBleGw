# -*- coding: utf-8 -*-
# 環境センサーをLimited Broadcasterモードにして
# 10秒アドバタイズ、170秒休み(開発中は50秒休み)に設定
# 常時スキャンし、データーを取得したらAmbientに送信する
# 複数台のセンサー端末に対応

from bluepy.btle import Peripheral, DefaultDelegate, Scanner, BTLEException, UUID
import bluepy.btle
import sys
import struct
from datetime import datetime
import argparse
import ambient
import requests
import time
import yaml

devices = {}

Debugging = False
def DBG(*args):
    if Debugging:
        msg = " ".join([str(a) for a in args])
        print(msg)
        sys.stdout.flush()

Verbose = False
def MSG(*args):
    if Verbose:
        msg = " ".join([str(a) for a in args])
        print(msg)
        sys.stdout.flush()

def sendWithRetry(am, data):
    for retry in range(6):  # 10秒間隔で6回リトライして、ダメならこの回は送信しない
        try:
            ret = am.send(data)
            MSG('sent to Ambient (ret = %d)' % ret.status_code)
            break
        except requests.exceptions.RequestException as e:
            MSG('request failed.')
            time.sleep(10)

def send2ambient(addr, am, dataRow):
    (temp, humid, press, systemp, vbat) = struct.unpack('<hhhhh', bytes.fromhex(dataRow))
    MSG(addr, temp / 100, humid / 100, press / 10, systemp / 100, vbat / 100)
    sendWithRetry(am, {'d1': temp / 100, 'd2': humid / 100, 'd3': press / 10, 'd4': systemp / 100, 'd5': vbat / 100})

class ScanDelegate(DefaultDelegate):
    def __init__(self):
        DefaultDelegate.__init__(self)
        self.lastseq = None
        self.lasttime = datetime.fromtimestamp(0)
        self.devs = {}

    def handleDiscovery(self, dev, isNewDev, isNewData):
        if isNewDev or isNewData:
            for (adtype, desc, value) in dev.getScanData():
                #print(adtype, desc, value, dev.addr)
                if desc == 'Manufacturer' and value[0:4] == 'ffff':
                    if dev.addr not in self.devs:
                        amconf = devices[dev.addr]
                        self.devs[dev.addr] = {
                            'lastseq': None,
                            'lasttime': datetime.fromtimestamp(0),
                            'ambient': ambient.Ambient(amconf['channelID'], amconf['writeKey']),
                        }
                    delta = datetime.now() - self.devs[dev.addr]['lasttime']
                    if value[4:6] != self.devs[dev.addr]['lastseq'] and delta.total_seconds() > 11: # アドバタイズする10秒の間に測定が実行されseqが加算されたものは捨てる
                        self.devs[dev.addr]['lastseq'] = value[4:6]
                        self.devs[dev.addr]['lasttime'] = datetime.now()
                        send2ambient(dev.addr, self.devs[dev.addr]['ambient'], value[6:])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store_true', help='debug msg on')
    parser.add_argument('-v', action='store_true', help='verbose msg on')
    parser.add_argument('-f', required=True, help='config')

    args = parser.parse_args(sys.argv[1:])

    global Debugging
    Debugging = args.d
    bluepy.btle.Debugging = args.d
    global Verbose
    Verbose = args.v

    with open(args.f, 'r') as f:
        conf = yaml.safe_load(f)
    global devices
    devices = conf['devices']

    scanner = Scanner().withDelegate(ScanDelegate())
    while True:
        try:
            scanner.scan(5.0) # スキャンする。デバイスを見つけた後の処理はScanDelegateに任せる
        except BTLEException:
            MSG('BTLE Exception while scannning.')

if __name__ == "__main__":
    main()
