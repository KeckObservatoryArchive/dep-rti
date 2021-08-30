'''
Define instrument keywords to monitor that indicate a new datafile was written.
- 'trigger' value indicates which keyword to monitor that will trigger processing.
- If 'val' is defined, trigger must equal val to initiate processing.
- If 'format' is defined, all keywords in curlies will be replaced by that keyword value.
- If format is not defined, the value of the trigger will be used.
- If zfill is defined, then left pad those keyword vals (assuming with '0')
- 'heartbeat' is keyword to use for heartbeat restart mechanism.  Second param is period in seconds to check.
'''

instr_keymap = {

    'koa': {
        'instr'    :  'KCWI',
        'trigger'  :  'LOUTFILE',
        'val'      :  None,
        'heartbeat': ['DISPCLK', 1],
        'transfer' : 0
    },

    'kfcs': {
        'instr'    :  'KCWI',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 30],
        'transfer' : 1
    },
    'kbds': {
        'instr'    :  'KCWI',
        'trigger'  :  'LOUTFILE',
        'val'      :  None,
        'heartbeat': ['ITERATION', 1],
        'transfer' : 1
    },

    'nids': {
        'instr'    :  'NIRES',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 600],
        'transfer' : 1
    },
    'nsds': {
        'instr'    :  'NIRES',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 600],
        'transfer' : 1
    },

    'deimosplus': {
        'instr'    :  'DEIMOS',
        'trigger'  :  'LASTCCD',
        'val'      :  None,
        'heartbeat':  ['UPTIME', 1],
        'transfer' : 1
    },
    'deifcs': {
        'instr'    :  'DEIMOS',
        'trigger'  :  'FCSIMGFI',
        'val'      :  None,
        'heartbeat':  '', # there is no keyword to track that this service is up
        'transfer' : 1
    },

    'esi': {
        'instr'    :  'ESI',
        'trigger'  :  'WDISK',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'heartbeat':  '', # there is no keyword to track that this service is up
        'transfer' : 0
    },

    'hiccd': {
        'instr'    :  'HIRES',
        'trigger'  :  'WDISK',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'heartbeat':  ['INFOMCLK', 1],
        'transfer' : 1
    },

    'lris': {
        'instr'    :  'LREDCCD',
        'trigger'  :  'LOUTFILE',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'heartbeat':  ['UPTIME', 1],
        'transfer' : 0
    },
    'lrisblue': {
        'instr'    :  'LRIS',
        'trigger'  :  'WDISK',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'probe':  '', # LRIS doesnt have any keywords to track uptime. LRIS upgrade should fix this
        'transfer' : 0
    },

    'mosfire': {
        'instr'    :  'MOSFIRE',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 1],
        'transfer' : 0
    },

    'alad': {
        'instr'    :  'NIRC2',
        'trigger'  :  'LASTFILE', # could alternatively be FILERDY with val==0
        'val'      :  None,
        'heartbeat':  '',  # alad doesn't have any keyword to track how long it's been up
        'transfer' : 0
    },

    'nspec': {
        'instr'    :  'NIRSPEC',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  'UPTIME',
        'transfer' : 0
    },
    'nscam': {
        'instr'    :  'NIRSPEC',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  'UPTIME',
        'transfer' : 0
    },

    'osiris': {
        'instr'    :  'OSIRIS',
        'trigger'  :  'ILASTFILE',
        'val'      :  None,
        'heartbeat':  'LASTALIVE',
        'transfer' : 0
    },
    'osiris???': {
        'instr'    :  'OSIRIS',
        'trigger'  :  'SLASTFILE',
        'val'      :  None,
        'heartbeat':  'LASTALIVE',
        'transfer' : 0
    }
}
