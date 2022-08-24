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
        'ktl_service':  'koa',
        'instr'      :  'KCWI',
        'trigger'    :  'LOUTFILE',
        'val'        :  None,
        'heartbeat'  : ['DISPCLK', 1],
        'transfer'   : 0
    },

    'kcwi_fcs': {
        'ktl_service':  'kfcs',
        'instr'      :  'KCWI',
        'trigger'    :  'LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['ITERATION', 30],
        'transfer'   : 1
    },
    'kcwi_blue': {
        'ktl_service':  'kbds',
        'instr'      :  'KCWI',
        'trigger'    :  'LOUTFILE',
        'val'        :  None,
        'heartbeat'  : ['ITERATION', 1],
        'transfer'   : 1
    },

    'nires_img': {
        'ktl_service':  'nids',
        'instr'      :  'NIRES',
        'trigger'    :  'LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['ITERATION', 600],
        'transfer'   : 1
    },
    'nires_spec': {
        'ktl_service':  'nsds',
        'instr'      :  'NIRES',
        'trigger'    :  'LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['ITERATION', 600],
        'transfer'   : 1
    },

    'deimos_spec': {
        'ktl_service':  'deimosplus',
        'instr'      :  'DEIMOS',
        'trigger'    :  'LASTCCD',
        'val'        :  None,
        'heartbeat'  :  ['UPTIME', 1],
        'transfer'   : 1
    },
    'deimos_fcs': {
        'ktl_service':  'deifcs',
        'instr'      :  'DEIMOS',
        'trigger'    :  'FCSIMGFI',
        'val'        :  None,
        'heartbeat'  :  '', # there is no keyword to track that this service is up
        'transfer'   : 1
    },

    'esi': {
        'ktl_service':  'esi',
        'instr'      :  'ESI',
        'trigger'    :  'WDISK',
        'val'        :  'false',
        'format'     :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'      :  {'LFRAMENO'  : 4},
        'heartbeat'  :  '', # there is no keyword to track that this service is up
        'transfer'   : 1
    },

    'hires': {
        'ktl_service':  'hiccd',
        'instr'      :  'HIRES',
        'trigger'    :  'WDISK',
        'val'        :  'false',
        'format'     :  '{OUTDIR}/{OUTFILE}{LFRAMENO}.fits',
        'zfill'      :  {'LFRAMENO'  : 4},
        'heartbeat'  :  ['INFOMCLK', 1],
        'transfer'   : 1
    },

    'lris_red': {
        'ktl_service':  'lredccd',
        'instr'      :  'LRIS',
        'trigger'    :  'LOUTFILE',
        'val'        :  None,
        'heartbeat'  :  ['UPTIME', 1],
        'transfer'   : 1
    },
    'lris_blue': {
        'ktl_service':  'lrisplus',
        'instr'      :  'LRIS',
        'trigger'    :  'BLUE_LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['UPTIME', 1], 
        'transfer'   : 1
    },
    'mosfire': {
        'ktl_service':  'mosfire',
        'instr'      :  'MOSFIRE',
        'trigger'    :  'LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['ITERATION', 1],
        'transfer'   : 1
    },

    'nirc2': {
        'ktl_service':  'nirc2plus',
        'instr'      :  'NIRC2',
        'trigger'    :  'LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['DISPCLK', 1],
        'delay'      : 1.0,
        'transfer'   : 1
    },

    'nirspec_spec': {
        'ktl_service':  'nspec',
        'instr'      :  'NIRSPEC',
        'trigger'    :  'LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['UPTIME', 1],
        'transfer'   : 1
    },
    'nirspec_scam': {
        'ktl_service':  'nscam',
        'instr'      :  'NIRSPEC',
        'trigger'    :  'LASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['DISP1CLK', 1],
        'transfer'   : 1
    },

    'osiris_spec': {
        'ktl_service':  'osiris',
        'instr'      :  'OSIRIS',
        'trigger'    :  'ILASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['ITERATION', 1],
        'transfer'   : 1
    },
    'osiris_img': {
        'ktl_service':  'osiris',
        'instr'      :  'OSIRIS',
        'trigger'    :  'SLASTFILE',
        'val'        :  None,
        'heartbeat'  :  ['ITERATION', 1],
        'transfer'   : 1
    }
}
