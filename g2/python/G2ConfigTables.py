# --python imports
import optparse
import sys
import os
import json


# ======================
class G2ConfigTables:

    # ----------------------------------------
    def __init__(self, configJson):
        self.configJsonDoc = configJson

    # ----------------------------------------
    # -- g2 specific calls
    # ----------------------------------------

    # ----------------------------------------
    def loadConfig(self, tableName):
        cfgDict = {}
        cfgDataRoot = json.loads(self.configJsonDoc)
        configNode = cfgDataRoot['G2_CONFIG']
        tableNode = configNode[tableName.upper()]
        for rowNode in tableNode:
            cfgNodeEntry = {}
            if tableName.upper() == 'CFG_DSRC':
                cfgNodeEntry['ID'] = rowNode['DSRC_ID']
                cfgNodeEntry['DSRC_CODE'] = rowNode['DSRC_CODE']
                cfgDict[cfgNodeEntry['DSRC_CODE']] = cfgNodeEntry
            elif tableName.upper() == 'CFG_ETYPE':
                cfgNodeEntry['ID'] = rowNode['ETYPE_ID']
                cfgNodeEntry['ETYPE_CODE'] = rowNode['ETYPE_CODE']
                cfgDict[cfgNodeEntry['ETYPE_CODE']] = cfgNodeEntry
            elif tableName.upper() == 'CFG_FTYPE':
                cfgNodeEntry['ID'] = rowNode['FTYPE_ID']
                cfgNodeEntry['FTYPE_CODE'] = rowNode['FTYPE_CODE']
                cfgNodeEntry['FTYPE_FREQ'] = rowNode['FTYPE_FREQ']
                cfgNodeEntry['FTYPE_EXCL'] = rowNode['FTYPE_EXCL']
                cfgNodeEntry['FTYPE_STAB'] = rowNode['FTYPE_STAB']
                cfgNodeEntry['DERIVED'] = rowNode['DERIVED']
                cfgDict[cfgNodeEntry['FTYPE_CODE']] = cfgNodeEntry
            elif tableName.upper() == 'CFG_ERRULE':
                cfgNodeEntry['ID'] = rowNode['ERRULE_ID']
                cfgNodeEntry['ERRULE_CODE'] = rowNode['ERRULE_CODE']
                cfgNodeEntry['REF_SCORE'] = rowNode['REF_SCORE']
                cfgNodeEntry['RTYPE_ID'] = rowNode['RTYPE_ID']
                cfgDict[cfgNodeEntry['ID']] = cfgNodeEntry
            elif tableName.upper() == 'CFG_ATTR':
                cfgNodeEntry['ATTR_ID'] = rowNode['ATTR_ID']
                cfgNodeEntry['ATTR_CODE'] = rowNode['ATTR_CODE']
                cfgNodeEntry['ATTR_CLASS'] = rowNode['ATTR_CLASS']
                cfgNodeEntry['FTYPE_CODE'] = rowNode['FTYPE_CODE']
                cfgNodeEntry['FELEM_CODE'] = rowNode['FELEM_CODE']
                cfgNodeEntry['FELEM_REQ'] = rowNode['FELEM_REQ']
                cfgDict[cfgNodeEntry['ATTR_CODE']] = cfgNodeEntry
            else:
                return None
        return cfgDict
