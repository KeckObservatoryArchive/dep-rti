import argparse
from astropy.io import fits
from astropy.visualization import ZScaleInterval, AsinhStretch
from astropy.visualization.mpl_normalize import ImageNormalize
import datetime as dt
from getpass import getuser
import matplotlib.pyplot as plt
import numpy as np
from os import walk, makedirs, chdir, remove
from os.path import isdir, isfile, basename
import requests
from socket import gethostname
import sys
from time import sleep
from common import create_logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import yaml
import gzip
import shutil
import logging


class KoaImagerDrp(FileSystemEventHandler):
    '''
    Handles the directory monitoring and processing of data.
    '''

    def __init__(self, instrument, datadir, outputdir, rti):

        self.running  = True
        self.whoami   = getuser()
        self.hostname = gethostname()
        self.rti      = rti
        self.rtiUrl   = None
        if isfile('config.live.ini'):
            with open('config.live.ini') as f:
                config = yaml.safe_load(f)
            if 'RTI' in config.keys():
                if 'API' in config['RTI'].keys():
                   self.rtiUrl  = config['RTI']['API']
                   self.rtiUser = config['RTI']['USER']
                   self.rtiPwd  = config['RTI']['PWD']

        self.instrument      = instrument
        self.datadir         = datadir
        self.outputdir       = outputdir
        self.calibrationsdir = f'./{self.instrument.lower()}_calibrations'

        name = f'koa.imager.{self.instrument.lower()}.lev1'
        logFile =  f'/log/{name}.log'
        self.logger = create_logger(name, logFile)

        self.dpi = 100

        self.queue         = []
        self.fileList      = []
        self.skipList      = []
        self.darkList      = {}
        self.darkFrame     = {}
        self.flatList      = {}
        self.flatoffList   = {}
        self.flatFrame     = {}
        self.flatoffFrame  = {}

        self.add_current_file_list()


    def on_any_event(self, event):
        '''
        Callback to add new files to the queue.
        '''

        # Skip if not a modified event
        if event.event_type != 'created':
            return

        # Check the base filename
        filename = basename(event.src_path)
        if not filename.endswith('.fits') or filename in self.fileList:
            return

        if self.instrument == 'NIRC2' and '_unp' in filename:
            return

        if self.instrument == 'OSIRIS' and not filename.startswith('OI'):
            return

        self.logger.info(f'on_any_event {event.src_path}')
        filename = event.src_path

        if filename in self.queue:
            self.logger.info('Skipping - already in queue')
            return

        if filename in self.fileList:
            self.logger.info('Skipping - already processed')
            return

        self.queue.append(filename)


    def add_current_file_list(self):
        '''
        Loops through and adds any files currently located in self.datadir
        to the queue.
        '''

        for root, dirs, files in walk(self.datadir):
            files.sort()
            for file in files:
                if not file.endswith('.fits'):
                    continue
                if self.instrument == 'NIRC2' and '_unp' in file:
                    continue
                if self.instrument == 'OSIRIS' and not file.startswith('OI'):
                    continue
                filename = f'{root}/{file}'
                self.queue.append(filename)

        self.running = False


    def process_current_file_list(self):
        '''
        Processes all files in the queue, then empties the queue.
        '''

        self.running = True

        while len(self.queue) > 0:
            filename = self.queue[0]
            self.process_file(filename)
            self.fileList.append(filename)
            self.queue.remove(filename)

        self.running = False


    def process_file(self, filename):
        '''
        Processes a file depending on its type.
        '''

        self.logger.info(f'Input file {filename}')

        try:
            header = fits.getheader(filename)
        except:
            self.logger.error(f'Error reading file ({filename})')
            return

        koaimtyp = header['KOAIMTYP']
        if   koaimtyp == 'dark':
            self.process_dark(filename, header)
        elif koaimtyp == 'flatlamp':
            self.process_flatlamp(filename, header)
        elif koaimtyp == 'flatlampoff':
            self.process_flatlampoff(filename, header)
        elif koaimtyp == 'object':
            self.process_object(filename, header)


    def get_key_list(self, filetype, header):
        '''
        Returns the configuration string based on file type.
        '''

        keys = ['NAXIS1', 'NAXIS2']
        if filetype == 'dark':
            if self.instrument == 'NIRC2':
                keys.extend(['SAMPMODE', 'MULTISAM', 'ITIME', 'COADDS'])
            elif self.instrument == 'OSIRIS':
                keys.extend(['ITIME', 'COADDS', 'READS'])
            name = 'dark'
        elif filetype == 'flat' or filetype == 'flatoff':
            if self.instrument == 'NIRC2':
                keys.append('CAMNAME')
            keys.append('FILTER')
            name = 'flat'
        for k in keys:
            name = f"{name}_{str(header[k]).replace(' ', '')}"

        return name


    def median_combine(self, filetype, name):
        '''
        Median combines the files located in a list based on file type.
        '''

        self.logger.info(f'Processing {filetype} frames for {name}')

        if filetype == 'dark':
            fileList = self.darkList[name]
        elif filetype == 'flat':
            fileList = self.flatList[name]
        elif filetype == 'flatoff':
            fileList = self.flatoffList[name]

        for file in fileList:
            self.logger.info(f'{file}')

        stack = [fits.getdata(file) for file in fileList]

        return np.median(stack, axis=0).astype('int32')


    def process_dark(self, filename, header):
        '''
        On new dark file, add to a list dependent on configuration string,
        and median combine when 5 have been found.
        '''

        name = self.get_key_list('dark', header)

        if name not in self.darkList.keys():
            self.darkList[name] = []

        self.darkList[name].append(filename)
        if len(self.darkList[name]) == 1:
            self.darkFrame[name] = fits.getdata(filename)

        if len(self.darkList[name]) >= 2:
            self.darkFrame[name] = self.median_combine('dark', name)
            hdu = fits.PrimaryHDU(header=header, data=self.darkFrame[name])
            hdu.writeto(f'{self.outputdir}/{name}.fits', overwrite=True)


    def process_flatlamp(self, filename, header):
        '''
        On new flat file, add to a list dependent on configuration string,
        and median combine when 5 have been found.
        '''

        name = self.get_key_list('flat', header)

        if name not in self.flatList.keys():
            self.flatList[name] = []

        self.flatList[name].append(filename)
        if len(self.flatList[name]) == 1:
            self.flatFrame[name] = fits.getdata(filename)

        if len(self.flatList[name]) >= 2:
            self.flatFrame[name] = self.median_combine('flat', name)


    def process_flatlampoff(self, filename, header):
        '''
        On new flatoff file, add to a list dependent on configuration string,
        and median combine when 5 have been found.
        '''

        name = self.get_key_list('flatoff', header)

        if name not in self.flatoffList.keys():
            self.flatoffList[name] = []

        self.flatoffList[name].append(filename)
        if len(self.flatoffList[name]) == 1:
            self.flatoffFrame[name] = fits.getdata(filename)

        if len(self.flatoffList[name]) >= 2:
            self.flatoffFrame[name] = self.median_combine('flatoff', name)


    def process_object(self, filename, header):
        '''
        Reduce the object frame.
         - dark subtract
         - divide by flat
         - write new FITS and JPG
        '''

        self.logger.info(f'Processing object ({filename})')
        name  = self.get_key_list('dark', header)
        name2 = self.get_key_list('flat', header)
        self.logger.info(f'{name} {name2}')

        # Does a dark or master dark exist?
        dark = True
        if name not in self.darkFrame.keys():
            dark = False
            masterDark = self.check_for_master('dark', name)
            if masterDark:
                self.darkFrame[name] = fits.getdata(masterDark)
                dark = True

        # Use flats taken today, if they exist
        flat = False
        if name2 in self.flatFrame.keys() and name2 in self.flatoffFrame.keys():
            self.logger.info(f'Creating normalized flat for {name2}')
            flatImg = self.flatFrame[name2] - self.flatoffFrame[name2]
            flatImg = flatImg / np.median(flatImg)
            flat = True
        # Otherwise look for a master flat
        else:
            masterFlat = self.check_for_master('flat', name2)
            if masterFlat:
                flatImg = fits.getdata(masterFlat)
                flat = True

        if flat == False:
            self.logger.info('Skipping - no flat found')
            if filename not in self.skipList:
                self.skipList.append(filename)
            return

        # Replace any zero or negative values
        flatImg = np.where(flatImg <= 0, 1, flatImg)

        newfile = basename(filename).replace('.fits', '_drp.fits')
        darkfile = newfile.replace('_drp', '_drp_dark')

        try:
            img  = fits.open(filename, ignore_missing_end=True)
        except:
            self.logger.info(f'Error reading file ({filename})')
            return
        hdr  = img[0].header
        data = img[0].data

        # Dark subtract and divide by flat
        if dark == True:
            self.logger.info(f'Dark subtracting object ({filename})')
            data = data - self.darkFrame[name]
            hdu = fits.PrimaryHDU(header=hdr, data=data.astype('int32'))
            hdu.writeto(f'{self.outputdir}/{darkfile}', overwrite=True)

        self.logger.info('Flat dividing image')
        data = data / flatImg

        # Create a new FITS file and JPG of the data
        self.logger.info(f'Creating new FITS file ({newfile})')
        hdr['DATLEVEL'] = 1
        hdu = fits.PrimaryHDU(header=hdr, data=data.astype('int32'))
        hdu.writeto(f'{self.outputdir}/{newfile}', overwrite=True)

        # gzip the FITS file
        self.logger.info(f'gzipping FITS file ({newfile})')
        gzipFile = f'{self.outputdir}/{newfile}.gz'
        with open(f'{self.outputdir}/{newfile}', 'rb') as fIn:
            with gzip.open(gzipFile, 'wb', compresslevel=1) as fOut:
                shutil.copyfileobj(fIn, fOut)
        remove(f'{self.outputdir}/{newfile}')

        newfile = newfile.replace('.fits', '.jpg')
        self.logger.info(f'Creating new JPG file ({newfile})')

        interval = ZScaleInterval()
        vmin, vmax = interval.get_limits(data)
        norm = ImageNormalize(vmin=vmin, vmax=vmax*3, stretch=AsinhStretch())

        figSize = (hdr['NAXIS1']/self.dpi,hdr['NAXIS2']/self.dpi)
        fig = plt.figure(figsize=figSize, frameon=False, dpi=self.dpi)
        ax = fig.add_axes([0,0,1,1])
        plt.axis('off')
        plt.imshow(np.flip(data, axis=0), cmap='gray', norm=norm)
        plt.savefig(f"{self.outputdir}/{newfile}")
        plt.close()

        # Call the RTI API
        if self.rti == True:
            try:
                koaid = hdr['KOAID']
                url = f'{self.rtiUrl}instrument={self.instrument}&koaid={koaid}'
                url = f'{url}&ingesttype=lev1&datadir={self.outputdir}'
                self.logger.info(f'Notifying RTI of reduction ({url})')
                resp = requests.get(url, auth=(self.rtiUser, self.rtiPwd))
            except:
                self.logger.info(f'Error with {url}')

        img.close()


    def check_for_master(self, filetype, name):
        '''
        Locates and returns the filename for a master calibration matching the
        filetype and configuration.
        '''

        # Check for master file
        filename = f'./{self.calibrationsdir}/{name}.fits'
        if isfile(filename):
            self.logger.info(f'Found master {filetype} for {name}')
            return filename

        return ''


def main():
    parser = argparse.ArgumentParser(description='Imaging Quicklook DRP')
    parser.add_argument('instrument', help='Instrument (NIRC2, OSIRIS)')
    parser.add_argument('datadir', help='Location of lev0 input data')
    parser.add_argument('outputdir', help='Location of lev1 output data')
    parser.add_argument('--rti', dest='rti', default=False, action='store_true',
                        help='Notify RTI upon each successful reduction')
    parser.add_argument('--manual', dest='manual', default=False,
                        action='store_true',
                        help='Manual run, disable end hour')
    args = parser.parse_args()

    instrument = args.instrument.upper()
    if instrument not in ['NIRC2', 'OSIRIS']:
        print(f'Instrument ({instrument}) not allowed')
        exit()

    # Wait for datadir to appear
    endHour = 17 if not args.manual else 24
    datadir = args.datadir
    print(f'Waiting for directory ({datadir}) to appear')
    while not isdir(datadir):
        hourNow = int(dt.datetime.utcnow().strftime('%H'))
        if hourNow >= endHour:
            print('Night is over - goodbye')
            exit()
        sleep(60)
    if datadir.endswith('/'):
        datadir = datadir[:-1]
    print(f'Directory ({datadir}) exists')

    outputdir = args.outputdir
    if outputdir.endswith('/'):
        outputdir = outputdir[:-1]
    if not isdir(outputdir):
        print(f'Directory ({outputdir}) does not exist - created')
        makedirs(outputdir)

    chdir(sys.path[0])

    event_handler = KoaImagerDrp(instrument, datadir, outputdir, args.rti)
    observer = Observer()
    observer.schedule(event_handler, path=datadir, recursive=False)
    observer.start()

    try:
        while True:
            hourNow = int(dt.datetime.utcnow().strftime('%H'))
            if event_handler.running == False:
                # Stop the DRP if 7am or later
                hourNow = int(dt.datetime.utcnow().strftime('%H'))
                if hourNow >= endHour:
                    event_handler.log.info('Shutting down')
                    observer.stop()
                    # Reprocess skipped incase morning cals taken
                    event_handler.log.info('Verifying any skipped files')
                    event_handler.queue = event_handler.skipList
                    event_handler.process_current_file_list()
                    event_handler.log.info('Goodbye')
                    return

                # Check the queue and process any files
                if len(event_handler.queue) > 0:
                    event_handler.log.info('Processing queue')
                    event_handler.process_current_file_list()

            sleep(10)
    except KeyboardInterrupt:
        observer.stop()

if __name__ == '__main__':
    main()
