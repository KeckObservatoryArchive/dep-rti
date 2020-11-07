#Define instrument keywords to monitor that indicate a new datafile was written.
#'trigger' value indicates which keyword to monitor that will trigger processing.
#If 'val' is defined, trigger must equal val to initiate processing.
#'format' defines the filepath construction from other keywords defined in 'fp_info'
#todo: Finish mapping all instrs
#todo: This could be put in each of the instr subclasses.
instr_keymap = {
    'KCWI': [
        {
            'service'  :  'kfcs',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  'LASTALIVE'
        },
        {
            'service'  :  'kbds',
            'trigger'  :  'LOUTFILE',
            'val'      :  None,
            'fp_info'  :  ['LOUTFILE'],
            'format'   :  lambda vals: f"{vals['LOUTFILE']}",
            'heartbeat':  'LASTALIVE'
        }
    ],
    'NIRES': [
        {
            'service'  :  'nids',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  'LASTALIVE'
        },
        {
            'service'  :  'nsds',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  'STATUS'
        }
    ],
    'DEIMOS': [
        {
            'service'  :  'deimosplus',
            'trigger'  :  'LASTCCD',
            'val'      :  None,
            'fp_info'  :  ['LASTCCD'],
            'format'   :  lambda vals: f"{vals['LASTCCD']}",
            'heartbeat':  'UPTIME'
        },
        {
            'service'  :  'deifcs',
            'trigger'  :  'FCSIMGFI',
            'val'      :  None,
            'fp_info'  :  ['FCSIMGFI'],
            'format'   :  lambda vals: f"{vals['FCSIMGFI']}",
            'heartbeat':  'UPTIME'
        }
    ],
    'ESI': [
        {
            'service'  :  'esi',
            'trigger'  :  'WDISK',
            'val'      :  'false',
            'fp_info'  :  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format'   :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits",
            'heartbeat':  '' # there is no keyword to track if the esi service is up
        }
    ],
    'HIRES': [
        {
            'service'  :  'hiccd',
            'trigger'  :  'WDISK',
            'val'      :  'false',
            'fp_info'  :  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format'   :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits",
            'heartbeat':  'INFOMCLK'
        }
    ],
    'LRIS': [
        {
            'service'  :  'lris',
            'trigger'  :  'WDISK',
            'val'      :  'false',
            'fp_info'  :  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format'   :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits",
            'heartbeat':  '' # LRIS doesnt have any keywords to track uptime. LRIS upgrade should fix this
        },
        {
            'service'  :  'lrisblue',
            'trigger'  :  'WDISK',
            'val'      :  'false',
            'fp_info'  :  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format'   :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits",
            'heartbeat':  '' # LRIS doesnt have any keywords to track uptime. LRIS upgrade should fix this
        }
    ],
    'MOSFIRE': [
        {
            'service'  :  'mosfire',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  'LASTALIVE'
        }
    ],
    'NIRC2': [
        {
            'service'  :  'alad',
            'trigger'  :  'LASTFILE', # could alternatively be FILERDY with val==0
            'val'      :  None,
            'fp_info'  :  ['OUTDIR', 'LASTFILE'],
            'format'   :  lambda vals: f"{vals['OUTDIR']}{vals['LASTFILE']}",
            'heartbeat':  ''  # alad doesn't have any keyword to track how long it's been up
        }
    ],
    'NIRSPEC': [
        {
            'service'  :  'nspec',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  'UPTIME'
        },
        {
            'service'  :  'nscam',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  'UPTIME'
        }
    ],
    'OSIRIS': [
        {
            'service'  :  'osiris',
            'trigger'  :  'ILASTFILE',
            'val'      :  None,
            'fp_info'  :  ['ILASTFILE'],
            'format'   :  lambda vals: f"{vals['ILASTFILE']}",
            'heartbeat':  'LASTALIVE'
        },
        {
            'service'  :  'osiris',
            'trigger'  :  'SLASTFILE',
            'val'      :  None,
            'fp_info'  :  ['SLASTFILE'],
            'format'   :  lambda vals: f"{vals['SLASTFILE']}",
            'heartbeat':  'LASTALIVE'
        }
    ]
}
