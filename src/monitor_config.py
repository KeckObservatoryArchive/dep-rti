'''
Define instrument keywords to monitor that indicate a new datafile was written.
- 'trigger' value indicates which keyword to monitor that will trigger processing.
- If 'val' is defined, trigger must equal val to initiate processing.
- 'format' defines the filepath construction from other keywords defined in 'fp_info'
- 'heartbeat' is keyword to use for heartbeat restart mechanism.  Second param is period to check.
- 'probe' is the keyword to read with Keyword.probe() to check that service is alive
  (NOTE: 'probe' must be a different keyword than 'trigger' b/c probe() read causes callback)
'''
#todo: This could be put in each of the instr subclasses.
instr_keymap = {
    'KCWI': [
        {
            'service'  :  'kfcs',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  ['ITERATION', 30]
        },
        {
            'service'  :  'kbds',
            'trigger'  :  'LOUTFILE',
            'val'      :  None,
            'fp_info'  :  ['LOUTFILE'],
            'format'   :  lambda vals: f"{vals['LOUTFILE']}",
            'heartbeat': ['ITERATION', 1]
        }
    ],
    'NIRES': [
        {
            'service'  :  'nids',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  ['ITERATION', 600]
        },
        {
            'service'  :  'nsds',
            'trigger'  :  'LASTFILE',
            'val'      :  None,
            'fp_info'  :  ['LASTFILE'],
            'format'   :  lambda vals: f"{vals['LASTFILE']}",
            'heartbeat':  ['ITERATION', 600]
        }
    ],
    'DEIMOS': [
        {
            'service'  :  'deimosplus',
            'trigger'  :  'LASTCCD',
            'val'      :  None,
            'fp_info'  :  ['LASTCCD'],
            'format'   :  lambda vals: f"{vals['LASTCCD']}",
            'heartbeat':  ['UPTIME', 1]
        },
        {
            'service'  :  'deifcs',
            'trigger'  :  'FCSIMGFI',
            'val'      :  None,
            'fp_info'  :  ['FCSIMGFI'],
            'format'   :  lambda vals: f"{vals['FCSIMGFI']}",
            'heartbeat':  '' # there is no keyword to track that this service is up
        }
    ],
    'ESI': [
        {
            'service'  :  'esi',
            'trigger'  :  'WDISK',
            'val'      :  'false',
            'fp_info'  :  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format'   :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits",
            'heartbeat':  '' # there is no keyword to track that this service is up
        }
    ],
    'HIRES': [
        {
            'service'  :  'hiccd',
            'trigger'  :  'WDISK',
            'val'      :  'false',
            'fp_info'  :  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format'   :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits",
            'heartbeat':  ['INFOMCLK', 1]
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
            'probe':  '' # LRIS doesnt have any keywords to track uptime. LRIS upgrade should fix this
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
