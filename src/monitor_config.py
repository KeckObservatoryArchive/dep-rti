#Define instrument keywords to monitor that indicate a new datafile was written.
#'trigger' value indicates which keyword to monitor that will trigger processing.
#If 'val' is defined, trigger must equal val to initiate processing.
#'format' defines the filepath construction from other keywords defined in 'fp_info'
#todo: Finish mapping all instrs
#todo: This could be put in each of the instr subclasses.
instr_keymap = {
    'KCWI': [
        {
            'service':  'kfcs',
            'trigger':  'LASTFILE',
            'val'    :  None,
            'fp_info':  ['LASTFILE'],
            'format' :  lambda vals: f"{vals['LASTFILE']}"
        },
        {
            'service':   'kbds',
            'trigger':  'LOUTFILE',
            'val'    :  None,
            'fp_info':  ['LOUTFILE'],
            'format' :  lambda vals: f"{vals['LOUTFILE']}"
        }
    ],
    'NIRES': [
        {
            'service':  'nids',
            'trigger':  'LASTFILE',
            'val'    :  None,
            'fp_info':  ['LASTFILE'],
            'format' :  lambda vals: f"{vals['LASTFILE']}"
        },
        {
            'service':  'nsds',
            'trigger':  'LASTFILE',
            'val'    :  None,
            'fp_info':  ['LASTFILE'],
            'format' :  lambda vals: f"/s{vals['LASTFILE']}"
        },
    ],
    'DEIMOS': [],
    'ESI': [],
    'HIRES': [
        {
            'service':  'hiccd',
            'trigger':  'WDISK',
            'val'    :  'false',
            'fp_info':  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format' :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits"
        },
    ],
    'LRIS': [],
    'MOSFIRE': [],
    'NIRC2': [],
    'NIRSPEC': [],
    'OSIRIS': [],
}
