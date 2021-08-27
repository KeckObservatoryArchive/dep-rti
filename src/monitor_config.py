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
        'heartbeat': ['DISPCLK', 1]
    },

    'kfcs': {
        'instr'    :  'KCWI',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 30]
    },
    'kbds': {
        'instr'    :  'KCWI',
        'trigger'  :  'LOUTFILE',
        'val'      :  None,
        'heartbeat': ['ITERATION', 1]
    },

    'nids': {
        'instr'    :  'NIRES',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 600]
    },
    'nsds': {
        'instr'    :  'NIRES',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 600]
    },

    'deimosplus': {
        'instr'    :  'DEIMOS',
        'trigger'  :  'LASTCCD',
        'val'      :  None,
        'heartbeat':  ['UPTIME', 1]
    },
    'deifcs': {
        'instr'    :  'DEIMOS',
        'trigger'  :  'FCSIMGFI',
        'val'      :  None,
        'heartbeat':  '' # there is no keyword to track that this service is up
    },

    'esi': {
        'instr'    :  'ESI',
        'trigger'  :  'WDISK',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'heartbeat':  '' # there is no keyword to track that this service is up
    },

    'hiccd': {
        'instr'    :  'HIRES',
        'trigger'  :  'WDISK',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'heartbeat':  ['INFOMCLK', 1]
    },

    'lris': {
        'instr'    :  'LRIS',
        'trigger'  :  'WDISK',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'heartbeat':  '' # LRIS doesnt have any keywords to track uptime. LRIS upgrade should fix this
    },
    'lrisblue': {
        'instr'    :  'lris',
        'trigger'  :  'WDISK',
        'val'      :  'false',
        'format'   :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'    :  {'LFRAMENO': 4},
        'probe':  '' # LRIS doesnt have any keywords to track uptime. LRIS upgrade should fix this
    },

    'mosfire': {
        'instr'    :  'MOSFIRE',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  ['ITERATION', 1]
    },

    'alad': {
        'instr'    :  'NIRC2',
        'trigger'  :  'LASTFILE', # could alternatively be FILERDY with val==0
        'val'      :  None,
        'heartbeat':  ''  # alad doesn't have any keyword to track how long it's been up
    },

    'nspec': {
        'instr'    :  'NIRSPEC',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  'UPTIME'
    },
    'nscam': {
        'instr'    :  'NIRSPEC',
        'trigger'  :  'LASTFILE',
        'val'      :  None,
        'heartbeat':  'UPTIME'
    },

    'osiris': {
        'instr'    :  'OSIRIS',
        'trigger'  :  'ILASTFILE',
        'val'      :  None,
        'heartbeat':  'LASTALIVE'
    },
    #???
    'osirisXXX': { 
        'instr'    :  'OSIRIS',
        'trigger'  :  'SLASTFILE',
        'val'      :  None,
        'heartbeat':  'LASTALIVE'
    }
}
