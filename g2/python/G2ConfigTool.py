#! /usr/bin/env python3

import argparse
import cmd
import glob
import json
import os
import pathlib
import re
import sys
import textwrap
import traceback
from collections import OrderedDict
from contextlib import suppress
import subprocess

try:
    import prettytable
except (ImportError, ModuleNotFoundError):
    prettytable = None

import G2Paths
from G2IniParams import G2IniParams
from senzing import G2Config, G2ConfigMgr, G2Exception

try:
    import readline
    import atexit
except ImportError:
    readline = None

try:
    from pygments import highlight, lexers, formatters

    pygmentsInstalled = True
except ImportError:
    pygmentsInstalled = False

# ===== supporting classes =====


# ==============================
class Colors:

    @classmethod
    def apply(cls, in_string, color_list=None):
        """apply list of colors to a string"""
        if color_list:
            prefix = "".join(
                [getattr(cls, i.strip().upper()) for i in color_list.split(",")]
            )
            suffix = cls.RESET
            return f"{prefix}{in_string}{suffix}"
        return in_string

    @classmethod
    def set_theme(cls, theme):
        # best for dark backgrounds
        if theme.upper() == "DEFAULT":
            cls.TABLE_TITLE = cls.FG_GREY42
            cls.ROW_TITLE = cls.FG_GREY42
            cls.COLUMN_HEADER = cls.FG_GREY42
            cls.ENTITY_COLOR = cls.FG_MEDIUMORCHID1
            cls.DSRC_COLOR = cls.FG_ORANGERED1
            cls.ATTR_COLOR = cls.FG_CORNFLOWERBLUE
            cls.GOOD = cls.FG_CHARTREUSE3
            cls.BAD = cls.FG_RED3
            cls.CAUTION = cls.FG_GOLD3
            cls.HIGHLIGHT1 = cls.FG_DEEPPINK4
            cls.HIGHLIGHT2 = cls.FG_DEEPSKYBLUE1
        elif theme.upper() == "LIGHT":
            cls.TABLE_TITLE = cls.FG_LIGHTBLACK
            cls.ROW_TITLE = cls.FG_LIGHTBLACK
            cls.COLUMN_HEADER = cls.FG_LIGHTBLACK  # + cls.ITALICS
            cls.ENTITY_COLOR = cls.FG_LIGHTMAGENTA + cls.BOLD
            cls.DSRC_COLOR = cls.FG_LIGHTYELLOW + cls.BOLD
            cls.ATTR_COLOR = cls.FG_LIGHTCYAN + cls.BOLD
            cls.GOOD = cls.FG_LIGHTGREEN
            cls.BAD = cls.FG_LIGHTRED
            cls.CAUTION = cls.FG_LIGHTYELLOW
            cls.HIGHLIGHT1 = cls.FG_LIGHTMAGENTA
            cls.HIGHLIGHT2 = cls.FG_LIGHTCYAN
        elif theme.upper() == "DARK":
            cls.TABLE_TITLE = cls.FG_LIGHTBLACK
            cls.ROW_TITLE = cls.FG_LIGHTBLACK
            cls.COLUMN_HEADER = cls.FG_LIGHTBLACK  # + cls.ITALICS
            cls.ENTITY_COLOR = cls.FG_MAGENTA + cls.BOLD
            cls.DSRC_COLOR = cls.FG_YELLOW + cls.BOLD
            cls.ATTR_COLOR = cls.FG_CYAN + cls.BOLD
            cls.GOOD = cls.FG_GREEN
            cls.BAD = cls.FG_RED
            cls.CAUTION = cls.FG_YELLOW
            cls.HIGHLIGHT1 = cls.FG_MAGENTA
            cls.HIGHLIGHT2 = cls.FG_CYAN

    # styles
    RESET = "\033[0m"
    BOLD = "\033[01m"
    DIM = "\033[02m"
    ITALICS = "\033[03m"
    UNDERLINE = "\033[04m"
    BLINK = "\033[05m"
    REVERSE = "\033[07m"
    STRIKETHROUGH = "\033[09m"
    INVISIBLE = "\033[08m"
    # foregrounds
    FG_BLACK = "\033[30m"
    FG_WHITE = "\033[97m"
    FG_BLUE = "\033[34m"
    FG_MAGENTA = "\033[35m"
    FG_CYAN = "\033[36m"
    FG_YELLOW = "\033[33m"
    FG_GREEN = "\033[32m"
    FG_RED = "\033[31m"
    FG_LIGHTBLACK = "\033[90m"
    FG_LIGHTWHITE = "\033[37m"
    FG_LIGHTBLUE = "\033[94m"
    FG_LIGHTMAGENTA = "\033[95m"
    FG_LIGHTCYAN = "\033[96m"
    FG_LIGHTYELLOW = "\033[93m"
    FG_LIGHTGREEN = "\033[92m"
    FG_LIGHTRED = "\033[91m"
    # backgrounds
    BG_BLACK = "\033[40m"
    BG_WHITE = "\033[107m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_YELLOW = "\033[43m"
    BG_GREEN = "\033[42m"
    BG_RED = "\033[41m"
    BG_LIGHTBLACK = "\033[100m"
    BG_LIGHTWHITE = "\033[47m"
    BG_LIGHTBLUE = "\033[104m"
    BG_LIGHTMAGENTA = "\033[105m"
    BG_LIGHTCYAN = "\033[106m"
    BG_LIGHTYELLOW = "\033[103m"
    BG_LIGHTGREEN = "\033[102m"
    BG_LIGHTRED = "\033[101m"
    # extended
    FG_DARKORANGE = "\033[38;5;208m"
    FG_SYSTEMBLUE = "\033[38;5;12m"  # darker
    FG_DODGERBLUE2 = "\033[38;5;27m"  # lighter
    FG_PURPLE = "\033[38;5;93m"
    FG_DARKVIOLET = "\033[38;5;128m"
    FG_MAGENTA3 = "\033[38;5;164m"
    FG_GOLD3 = "\033[38;5;178m"
    FG_YELLOW1 = "\033[38;5;226m"
    FG_SKYBLUE1 = "\033[38;5;117m"
    FG_SKYBLUE2 = "\033[38;5;111m"
    FG_ROYALBLUE1 = "\033[38;5;63m"
    FG_CORNFLOWERBLUE = "\033[38;5;69m"
    FG_HOTPINK = "\033[38;5;206m"
    FG_DEEPPINK4 = "\033[38;5;89m"
    FG_MAGENTA3 = "\033[38;5;164m"
    FG_SALMON = "\033[38;5;209m"
    FG_MEDIUMORCHID1 = "\033[38;5;207m"
    FG_NAVAJOWHITE3 = "\033[38;5;144m"
    FG_DARKGOLDENROD = "\033[38;5;136m"
    FG_STEELBLUE1 = "\033[38;5;81m"
    FG_GREY42 = "\033[38;5;242m"
    FG_INDIANRED = "\033[38;5;131m"
    FG_DEEPSKYBLUE1 = "\033[38;5;39m"
    FG_ORANGE3 = "\033[38;5;172m"
    FG_RED3 = "\033[38;5;124m"
    FG_SEAGREEN2 = "\033[38;5;83m"
    FG_YELLOW3 = "\033[38;5;184m"
    FG_CYAN3 = "\033[38;5;43m"
    FG_CHARTREUSE3 = "\033[38;5;70m"
    FG_ORANGERED1 = "\033[38;5;202m"


def colorize(in_string, color_list="None"):
    return Colors.apply(in_string, color_list)


def colorize_msg(msg_text, msg_type_or_color=""):
    if msg_type_or_color.upper() == "ERROR":
        msg_color = "bad"
    elif msg_type_or_color.upper() == "WARNING":
        msg_color = "caution,italics"
    elif msg_type_or_color.upper() == "INFO":
        msg_color = "highlight2"
    elif msg_type_or_color.upper() == "SUCCESS":
        msg_color = "good"
    else:
        msg_color = msg_type_or_color
    print(f"\n{Colors.apply(msg_text, msg_color)}\n")


def colorize_json(json_str):
    for token in set(re.findall(r'"(.*?)"', json_str)):
        tag = f'"{token}":'
        if tag in json_str:
            json_str = json_str.replace(tag, colorize(tag, "attr_color"))
        else:
            tag = f'"{token}"'
            if tag in json_str:
                json_str = json_str.replace(tag, colorize(tag, "dim"))
    return json_str


# ===== main class =====


class G2CmdShell(cmd.Cmd, object):

    def __init__(self, g2module_params, hist_disable, force_mode, file_to_process):
        cmd.Cmd.__init__(self)

        # Cmd Module settings
        self.intro = ""
        self.prompt = "(g2cfg) "
        self.ruler = "-"
        self.doc_header = "Configuration Command List"
        self.misc_header = "Help Topics (help <topic>)"
        self.undoc_header = "Misc Commands"
        self.__hidden_methods = (
            "do_EOF",
            "do_help",
            "do_addStandardizeFunc",
            "do_addExpressionFunc",
            "do_addComparisonFunc",
            "do_addComparisonFuncReturnCode",
            "do_addFeatureComparison",
            "do_deleteFeatureComparison",
            "do_addFeatureComparisonElement",
            "do_deleteFeatureComparisonElement",
            "do_addFeatureDistinctCallElement",
            "do_setFeatureElementDerived",
            "do_setFeatureElementDisplayLevel",
            "do_addEntityScore",
            "do_addToNameSSNLast4hash",
            "do_deleteFromSSNLast4hash",
            "do_updateAttributeAdvanced",
            "do_updateAttributeAdvanced",
            "do_updateFeatureVersion",
        )

        self.g2_configmgr = G2ConfigMgr()
        self.g2_config = G2Config()

        # Set flag to know if running an interactive command shell or reading from file
        self.isInteractive = True

        # Config variables and setup
        self.configUpdated = False
        self.g2_module_params = g2module_params

        # Processing input file
        self.forceMode = force_mode
        self.fileToProcess = file_to_process

        self.attributeClassList = (
            "NAME",
            "ATTRIBUTE",
            "IDENTIFIER",
            "ADDRESS",
            "PHONE",
            "RELATIONSHIP",
            "OTHER",
        )
        self.lockedFeatureList = (
            "NAME",
            "ADDRESS",
            "PHONE",
            "DOB",
            "REL_LINK",
            "REL_ANCHOR",
            "REL_POINTER",
        )
        self.valid_behavior_codes = [
            "NAME",
            "A1",
            "A1E",
            "A1ES",
            "F1",
            "F1E",
            "F1ES",
            "FF",
            "FFE",
            "FFES",
            "FM",
            "FME",
            "FMES",
            "FVM",
            "FVME",
            "FVMES",
            "NONE",
        ]

        self.json_attr_types = {
            "ID": "integer",
            "EXECORDER": "integer",
            "DATASOURCE": "string|255",
            "FEATURE": "string|255",
            "ELEMENT": "string|255",
            "ATTRIBUTE": "string|255",
            "FRAGMENT": "string|255",
            "RULE": "string|255",
            "TIER": "integer",
            "RTYPEID": "integer",
            "REF_SCORE": "integer",
            "FUNCTION": "string|255",
            "SECTION": "string|255",
            "FIELD": "string|255",
        }

        # Setup for pretty printing
        Colors.set_theme("DEFAULT")
        self.pygmentsInstalled = True if "pygments" in sys.modules else False
        # self.current_output_format = 'table' if prettytable else 'jsonl'
        self.current_output_format_list = "table" if prettytable else "jsonl"
        self.current_output_format_record = "json"

        # Readline and history
        self.readlineAvail = True if "readline" in sys.modules else False
        self.histDisable = hist_disable
        self.histCheck()

        self.parser = argparse.ArgumentParser(prog="", add_help=False)
        self.subparsers = self.parser.add_subparsers()

        getConfig_parser = self.subparsers.add_parser(
            "getConfig", usage=argparse.SUPPRESS
        )
        getConfig_parser.add_argument("configID", type=int)

    # ===== custom help section =====

    def do_help(self, help_topic):
        """"""
        if not help_topic:
            self.help_overview()
            return

        if help_topic not in self.get_names(include_hidden=True):
            help_topic = "do_" + help_topic
            if help_topic not in self.get_names(include_hidden=True):
                cmd.Cmd.do_help(self, help_topic[3:])
                return

        topic_docstring = getattr(self, help_topic).__doc__
        if not topic_docstring:
            colorize_msg(f"No help found for {help_topic}", "warning")
            return

        help_text = current_section = ""
        headers = [
            "Syntax:",
            "Examples:",
            "Example:",
            "Notes:",
            "Caution:",
            "Arguments:",
        ]
        help_lines = textwrap.dedent(topic_docstring).split("\n")

        for line in help_lines:
            line_color = ""
            if line:
                if line in headers:
                    line_color = "highlight2"
                    current_section = line

                if current_section == "Caution:":
                    line_color = "caution, italics"
                elif current_section not in (
                    "Syntax:",
                    "Examples:",
                    "Example:",
                    "Notes:",
                    "Arguments:",
                ):
                    line_color = ""

            if re.match(rf"^\s*{help_topic[3:]}", line) and not line_color:
                sep_column = line.find(help_topic[3:]) + len(help_topic[3:])
                help_text += (
                    line[0:sep_column] + colorize(line[sep_column:], "dim") + "\n"
                )
            else:
                help_text += colorize(line, line_color) + "\n"

        print(help_text)

    def help_all(self):
        args = ("",)
        cmd.Cmd.do_help(self, *args)

    def help_overview(self):
        print(
            textwrap.dedent(
                f"""
        {colorize('This utility allows you to configure a Senzing instance.', '')}

        {colorize('Senzing compares records within and across data sources.  Records consist of features and features have attributes.', '')}
        {colorize('For instance, the NAME feature has attributes such as NAME_FIRST and NAME_LAST for a person and NAME_ORG for an', '')}
        {colorize('organization.', '')}

        {colorize('Features are standardized and expressed in various ways to create candidate keys, and when candidates are found all', '')}
        {colorize('of their features are compared to the features of the incoming record to see how close they actually are.', '')}

        {colorize('Finally, a set of rules or "principles" are applied to the feature scores of each candidate to see if the incoming', '')}
        {colorize('record should resolve to an existing entity or become a new one. In either case, the rules are also used to create', '')}
        {colorize('relationships between entities.', '')}

        {colorize('Additional help:', 'highlight2')}
            help basic      {colorize('<- for commonly used commands', 'dim')}
            help features   {colorize('<- to be used only with the guidance of Senzing support', 'dim')}
            help principles {colorize('<- to be used only with the guidance of Senzing support', 'dim')}
            help all        {colorize('<- to show all configuration commands', 'dim')}

        {colorize('To understand more about configuring Senzing, please review:', '')}
            {colorize('https://senzing.com/wp-content/uploads/Entity-Resolution-Processes-021320.pdf', 'highlight1, underline')}
            {colorize('https://senzing.com/wp-content/uploads/Principle-Based-Entity-Resolution-092519.pdf', 'highlight1, underline')}
            {colorize('https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping', 'highlight1, underline')}

        """
            )
        )

    def help_basic(self):
        print(
            textwrap.dedent(
                f"""
        {colorize('Senzing comes pre-configured with all the settings needed to resolve persons and organizations.  Usually all that is required', '')}
        {colorize('is for you to register your data sources and start loading data based on the Generic Entity Specification.', '')}

        {colorize('Data source commands:', 'highlight2')}
            addDataSource           {colorize('<- to register a new data source', 'dim')}
            deleteDataSource        {colorize('<- to remove a data source created by error', 'dim')}
            listDataSources         {colorize('<- to see all the registered data sources', 'dim')}

        {colorize('When you see a how or a why screen output in Senzing, you see the actual entity counts and scores of a match. The list functions', 'dim,italics')}
        {colorize('below show you what those thresholds and scores are currently configured to.', 'dim,italics')}

        {colorize('Features and attribute settings:', 'highlight2')}
            listFeatures            {colorize('<- to see all features, whether they are used for candidates, and how they are scored', 'dim')}
            listAttributes          {colorize('<- to see all the attributes you can map to', 'dim')}

        {colorize('Principles (rules, scores, and thresholds):', 'highlight2')}
            listFunctions           {colorize('<- to see all the standardize, expression and comparison functions possible', 'dim')}
            listGenericThresholds   {colorize('<- to see all the thresholds for when feature values go generic for candidates or scoring', 'dim')}
            listRules               {colorize('<- to see all the principles in the order they are evaluated', 'dim')}
            listFragments           {colorize('<- to see all the fragments of rules are configured, such as what is considered close_name', 'dim')}

        {colorize('CAUTION:', 'caution, italics')}
            {colorize('While adding or updating features, expressions, scoring thresholds and rules are discouraged without the guidance of Senzing support,', 'caution, italics')}
            {colorize('knowing how they are configured and what their thresholds are can help you understand why records resolved or not, leading to the', 'caution, italics')}
            {colorize('proper course of action when working with Senzing Support.', 'caution, italics')}

        """
            )
        )

    def help_features(self):
        print(
            textwrap.dedent(
                f"""
        {colorize('New features and their attributes are rarely needed.  But when they are they are usually industry specific', '')}
        {colorize('identifiers (F1s) like medicare_provider_id or swift_code for a bank.  If you want some other kind of attribute like a grouping (FF)', '')}
        {colorize('or a physical attribute (FME, FMES), it is best to clone an existing feature by doing a getFeature, then modifying the json payload to', '')}
        {colorize('use it in an addFeature.', '')}

        {colorize('Commands to add or update features:', 'highlight2')}
            listFeatures            {colorize('<- to list all the features in the system', 'dim')}
            getFeature              {colorize('<- get the json configuration for an existing feature', 'dim')}
            addFeature              {colorize('<- add a new feature from a json configuration', 'dim')}
            setFeature              {colorize('<- to change a setting on an existing feature', 'dim')}
            deleteFeature           {colorize('<- to delete a feature added by mistake', 'dim')}

        {colorize('Attributes are what you map your source data to.  If you add a new feature, you will also need to add attributes for it. Be sure to', '')}
        {colorize('use a unique ID for attributes and to classify them as either an ATTRIBUTE or an IDENTIFIER.', '')}

        {colorize('Commands to add or update attributes:', 'highlight2')}
            listAttributes          {colorize('<- to see all the attributes you can map to', 'dim')}
            getAttribute            {colorize('<- get the json configuration for an existing attribute', 'dim')}
            addAttribute            {colorize('<- add a new attribute from a json configuration', 'dim')}
            deleteAttribute         {colorize('<- to delete an attribute added by mistake', 'dim')}

        {colorize('Some templates have been created to help you add new identifiers if needed. A template adds a feature and its required', '')}
        {colorize('attributes with one command.', '')}

        {colorize('Commands for using templates:', 'highlight2')}
            templateAdd             {colorize('<- add an identifier (F1) feature and attributes based on a template', 'dim')}
            templateAdd list        {colorize('<- to see the list of available templates', 'dim')}
        """
            )
        )

    def help_principles(self):
        print(
            textwrap.dedent(
                f"""
        {colorize('Before the principles are applied, the features and expressions created for an incoming record are used to find candidates.', '')}
        {colorize('An example of an expression is name and DOB and there is an expression call on the feature "name" to automatically create it', '')}
        {colorize('if both a name and DOB are present on the incoming record.  Features and expressions used for candidates are also referred', '')}
        {colorize('to as candidate builders or candidate keys.', '')}

        {colorize('Commands that help with configuring candidate keys:', 'highlight2')}
            listFeatures            {colorize('<- to see what features are used for candidates', 'dim')}
            setFeature              {colorize('<- to toggle whether or not a feature is used for candidates', 'dim')}
            listExpressionCalls     {colorize('<- to see what expressions are currently being created', 'dim')}
            addToNamehash           {colorize('<- to add an element from another feature to the list of composite name keys', 'dim')}
            addExpressionCall       {colorize('<- to add a new expression call, aka candidate key', 'dim')}
            listGenericThresholds   {colorize('<- to see when candidate keys will become generic and are no longer used to find candidates', 'dim')}
            setGenericThreshold     {colorize('<- to change when features with certain behaviors become generic', 'dim')}

        {colorize('CAUTION:', 'caution, italics')}
            {colorize('The cost of raising generic thresholds is speed. It is always best to keep generic thresholds low and to add new', 'caution, italics')}
            {colorize('new expressions instead.  You can extend composite key expressions with the addToNameHash command above, or add ', 'caution, italics')}
            {colorize('new expressions by using the addExpressionCall command above.', 'caution, italics')}

        {colorize('Once the candidate matches have been found, scoring and rule evaluation takes place.  Scores are rolled up by behavior.', '')}
        {colorize('For instance, both addresses and phones have the behavior FF (Frequency Few). If they both score above their scoring', '')}
        {colorize('function''s close threshold, there would be two CLOSE_FFs (a fragment) which can be used in a rule such as NAME+CLOSE_FF.', '')}

        {colorize('Commands that help with configuring principles (rules) and scoring:', 'highlight2')}
            listRules               {colorize('<- these are the principles that are applied top down', 'dim')}
            listFragments           {colorize('<- rules are combinations of fragments like close_name or same_name', 'dim')}
            listFunctions           {colorize('<- the comparison functions show you what is considered same, close, likely, etc.', 'dim')}
            setRule                 {colorize('<- to change whether an existing rule resolves or relates', 'dim')}
        """
            )
        )

    def help_support(self):
        print(
            textwrap.dedent(
                f"""
        {colorize('Senzing Knowledge Center:', 'dim')} {colorize('https://senzing.zendesk.com/hc/en-us', 'highlight1,underline')}

        {colorize('Senzing Support Request:', 'dim')} {colorize('https://senzing.zendesk.com/hc/en-us/requests/new', 'highlight1,underline')}
        """
            )
        )

    # ===== Auto completion section =====

    def get_names(self, include_hidden=False):
        """Override base method to return methods for autocomplete and help"""
        if not include_hidden:
            return [n for n in dir(self.__class__) if n not in self.__hidden_methods]
        return list(dir(self.__class__))

    def completenames(self, text, *ignored):
        """Override function from cmd module to make command completion case insensitive"""
        dotext = "do_" + text
        return [a[3:] for a in self.get_names() if a.lower().startswith(dotext.lower())]

    def complete_exportToFile(self, text, line, begidx, endidx):
        if re.match("exportToFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, "exportToFile")

    def complete_importFromFile(self, text, line, begidx, endidx):
        if re.match("importFromFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, "importFromFile")

    def pathCompletes(self, text, line, begidx, endidx, callingcmd):
        """Auto complete paths for commands that have a complete_ function"""

        completes = []

        pathComp = line[len(callingcmd) + 1 : endidx]
        fixed = line[len(callingcmd) + 1 : begidx]

        for path in glob.glob(f"{pathComp}*"):
            path = (
                path + os.sep
                if path and os.path.isdir(path) and path[-1] != os.sep
                else path
            )
            completes.append(path.replace(fixed, "", 1))

        return completes

    def complete_getAttribute(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_ATTR", "ATTR_CODE", text)

    def complete_getFeature(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_FTYPE", "FTYPE_CODE", text)

    def complete_getElement(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_FELEM", "FELEM_CODE", text)

    def complete_getFragment(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_ERFRAG", "ERFRAG_CODE", text)

    def complete_getRule(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_ERRULE", "ERRULE_CODE", text)

    def complete_deleteAttribute(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_ATTR", "ATTR_CODE", text)

    def complete_deleteDataSource(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_DSRC", "DSRC_CODE", text)

    def complete_deleteElement(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_FELEM", "FELEM_CODE", text)

    def complete_deleteEntityType(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_ETYPE", "ETYPE_CODE", text)

    def complete_deleteFeature(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_FTYPE", "FTYPE_CODE", text)

    def complete_deleteFeatureComparison(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_FTYPE", "FTYPE_CODE", text)

    def complete_deleteFeatureDistinctCall(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_FTYPE", "FTYPE_CODE", text)

    def complete_deleteFragment(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_ERFRAG", "ERFRAG_CODE", text)

    def complete_deleteRule(self, text, line, begidx, endidx):
        return self.codes_completes("CFG_ERRULE", "ERRULE_CODE", text)

    def codes_completes(self, table, field, arg):
        # Build list each time to have latest even after an add*, delete*
        return [
            code
            for code in self.getRecordCodes(table, field)
            if code.lower().startswith(arg.lower())
        ]

    def getRecordCodes(self, table, field):
        code_list = []
        for i in range(len(self.cfgData["G2_CONFIG"][table])):
            code_list.append(self.cfgData["G2_CONFIG"][table][i][field])
        return code_list

    def complete_getConfigSection(self, text, line, begidx, endidx):
        return [
            section
            for section in self.cfgData["G2_CONFIG"].keys()
            if section.lower().startswith(text.lower())
        ]

    # ===== command history section =====

    def histCheck(self):

        self.histFileName = None
        self.histFileError = None
        self.histAvail = False

        if not self.histDisable:

            if readline:
                tmpHist = "." + os.path.basename(
                    sys.argv[0].lower().replace(".py", "_history")
                )
                self.histFileName = os.path.join(os.path.expanduser("~"), tmpHist)

                # Try and open history in users home first for longevity
                try:
                    open(self.histFileName, "a").close()
                except IOError as e:
                    self.histFileError = f"{e} - Couldn't use home, trying /tmp/..."

                # Can't use users home, try using /tmp/ for history useful at least in the session
                if self.histFileError:

                    self.histFileName = f"/tmp/{tmpHist}"
                    try:
                        open(self.histFileName, "a").close()
                    except IOError as e:
                        self.histFileError = f"{e} - User home dir and /tmp/ failed"
                        return

                hist_size = 2000
                readline.read_history_file(self.histFileName)
                readline.set_history_length(hist_size)
                atexit.register(readline.set_history_length, hist_size)
                atexit.register(readline.write_history_file, self.histFileName)

                self.histFileName = self.histFileName
                self.histFileError = None
                self.histAvail = True

    def do_histDedupe(self, arg):
        """
        Deduplicates the command history

        Syntax:
            histDedupe
        """
        if self.histAvail:
            if (
                input(
                    "\nAre you sure you want to de-duplicate the session history? (y/n) "
                )
                .upper()
                .startwith("Y")
            ):

                with open(self.histFileName) as hf:
                    linesIn = (line.rstrip() for line in hf)
                    uniqLines = OrderedDict.fromkeys(line for line in linesIn if line)

                    readline.clear_history()
                    for ul in uniqLines:
                        readline.add_history(ul)

                colorize_msg(
                    "Session history and history file both deduplicated", "success"
                )
            else:
                print()
        else:
            colorize_msg("History is not available in this session", "warning")

    def do_histClear(self, arg):
        """
        Clears the command history

        Syntax:
            histClear
        """
        if self.histAvail:
            if (
                input("\nAre you sure you want to clear the session history? (y/n) ")
                .upper()
                .startwith("Y")
            ):
                readline.clear_history()
                readline.write_history_file(self.histFileName)
                colorize_msg("Session history and history file both cleared", "success")
            else:
                print()
        else:
            colorize_msg("History is not available in this session", "warning")

    def do_history(self, arg):
        """
        Displays the command history

        Syntax:
            history
        """
        if self.histAvail:
            print()
            for i in range(readline.get_current_history_length()):
                print(readline.get_history_item(i + 1))
            print()
        else:
            colorize_msg("History is not available in this session.", "warning")

    # ===== command loop section =====

    def initEngines(self, init_msg=False):

        if init_msg:
            colorize_msg("Initializing Senzing engines ...")

        try:
            self.g2_configmgr.init("pyG2ConfigMgr", self.g2_module_params, False)
            self.g2_config.init("pyG2Config", self.g2_module_params, False)
        except G2Exception as err:
            colorize_msg(err, "error")
            self.destroyEngines()
            sys.exit(1)

        # Re-read config after a save
        if self.configUpdated:
            self.loadConfig()

    def destroyEngines(self):

        with suppress(Exception):
            self.g2_configmgr.destroy()
            self.g2_config.destroy()

    def loadConfig(self, defaultConfigID=None):

        # Get the current configuration from the Senzing database
        if not defaultConfigID:
            defaultConfigID = bytearray()
            self.g2_configmgr.getDefaultConfigID(defaultConfigID)

        # If a default config isn't found, create a new default configuration
        if not defaultConfigID:

            colorize_msg("Adding default config to new database", "warning")

            config_handle = self.g2_config.create()

            config_default = bytearray()
            self.g2_config.save(config_handle, config_default)
            config_string = config_default.decode()

            # Persist new default config to Senzing Repository
            try:
                addconfig_id = bytearray()
                self.g2_configmgr.addConfig(
                    config_string,
                    "New default configuration added by G2ConfigTool.",
                    addconfig_id,
                )
                self.g2_configmgr.setDefaultConfigID(addconfig_id)
            except G2Exception:
                raise

            colorize_msg("Default config added!", "success")
            self.destroyEngines()
            self.initEngines(init_msg=(True if self.isInteractive else False))
            defaultConfigID = bytearray()
            self.g2_configmgr.getDefaultConfigID(defaultConfigID)

        config_current = bytearray()
        self.g2_configmgr.getConfig(defaultConfigID, config_current)
        self.cfgData = json.loads(config_current.decode())
        self.configUpdated = False

    def preloop(self):

        self.initEngines()
        self.loadConfig()

        colorize_msg(
            "Welcome to the Senzing configuration tool! Type help or ? to list commands",
            "highlight2",
        )

    def cmdloop(self):

        while True:
            try:
                cmd.Cmd.cmdloop(self)
                break
            except KeyboardInterrupt:
                if self.configUpdated:
                    if (
                        input(
                            "\n\nThere are unsaved changes, would you like to save first? (y/n) "
                        )
                        .upper()
                        .startswith("Y")
                    ):
                        self.do_save(self)
                        break

                if (
                    input("\nAre you sure you want to exit? (y/n) ")
                    .upper()
                    .startswith("Y")
                ):
                    break
                else:
                    print()

            except TypeError as err:
                colorize_msg(err, "error")
                type_, value_, traceback_ = sys.exc_info()
                for item in traceback.format_tb(traceback_):
                    print(item)

    def postloop(self):
        self.destroyEngines()

    def emptyline(self):
        return

    def default(self, line):
        colorize_msg("Unknown command, type help to list available commands", "warning")
        return

    def fileloop(self):

        # Get initial config
        self.initEngines(init_msg=False)
        self.loadConfig()

        # Set flag to know running an interactive command shell or not
        self.isInteractive = False

        save_detected = False

        with open(self.fileToProcess) as data_in:
            for line in data_in:
                line = line.strip()
                if len(line) > 0 and line[0:1] not in ("#", "-", "/"):
                    # *args allows for empty list if there are no args
                    (read_cmd, *args) = line.split()
                    process_cmd = f"do_{read_cmd}"
                    print(colorize(f"----- {read_cmd} -----", "dim"))
                    print(line)

                    if process_cmd == "do_save" and not save_detected:
                        save_detected = True

                    if process_cmd not in dir(self):
                        colorize_msg(f"Command {read_cmd} not found", "error")
                    else:
                        exec_cmd = f"self.{process_cmd}('{' '.join(args)}')"
                        exec(exec_cmd)

                    if not self.forceMode:
                        if (
                            input("\nPress enter to continue or (Q)uit... ")
                            .upper()
                            .startswith("Q")
                        ):
                            break

        if not save_detected and self.configUpdated:
            if not self.forceMode:
                if (
                    input(
                        "\nNo save command was issued would you like to save now? (y/n) "
                    )
                    .upper()
                    .startswith("Y")
                ):
                    self.do_save(self)
                    print()
                    return

            colorize_msg("Configuration changes were not saved!", "warning")

    def do_quit(self, arg):
        if self.configUpdated:
            if (
                input(
                    "\nThere are unsaved changes, would you like to save first? (y/n) "
                )
                .upper()
                .startswith("Y")
            ):
                self.do_save(self)
            else:
                colorize_msg("Configuration changes were not saved!", "warning")
        print()
        return True

    def do_exit(self, arg):
        self.do_quit(self)
        return True

    def do_save(self, args):
        if self.configUpdated:

            # If not accepting file commands without prompts and not using older style config file
            if not self.forceMode:
                if (
                    not input(
                        "\nAre you certain you wish to proceed and save changes? (y/n) "
                    )
                    .upper()
                    .startswith("Y")
                ):
                    colorize_msg("Configuration changes have not been saved", "warning")
                    return

            try:
                newConfigId = bytearray()
                self.g2_configmgr.addConfig(
                    json.dumps(self.cfgData), "Updated by G2ConfigTool", newConfigId
                )
                self.g2_configmgr.setDefaultConfigID(newConfigId)

            except G2Exception as err:
                colorize_msg(err, "error")
            else:
                colorize_msg("Configuration changes saved!", "success")
                # Reinit engines to pick up changes. This is good practice and will be needed when rewritten to fully use cfg APIs
                # Don't display init msg if not interactive (fileloop)
                if sys._getframe().f_back.f_code.co_name == "onecmd":
                    self.destroyEngines()
                    self.initEngines(init_msg=(True if self.isInteractive else False))
                    self.configUpdated = False

        else:
            colorize_msg("There were no changes to save", "warning")

    def do_shell(self, line):
        output = os.popen(line).read()
        print(f"\n{output}\n")

    # ===== json configuration file section =====

    def do_getDefaultConfigID(self, arg):
        """
        Returns the the current configuration ID

        Syntax:
            getDefaultConfigID
        """
        response = bytearray()
        try:
            self.g2_configmgr.getDefaultConfigID(response)
            colorize_msg(f"The default config ID is: {response.decode()}")
        except G2Exception as err:
            colorize_msg(err, "error")

    def do_getConfigList(self, arg):
        """
        Returns the list of all known configurations

        Syntax:
            getConfigList [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        try:
            response = bytearray()
            self.g2_configmgr.getConfigList(response)
            self.print_json_record(json.loads(response.decode())["CONFIGS"])
        except G2Exception as err:
            colorize_msg(err, "error")

    def do_reloadConfig(self, arg):
        """
        Reload the configuration, abandoning any changes

        Syntax:
            configReload
        """
        if self.configUpdated:
            if (
                not input(
                    "\nYou have unsaved changes, are you sure you want to discard them? (y/n) "
                )
                .upper()
                .startswith("Y")
            ):
                colorize_msg("Your changes have not been overwritten", "info")
                return
            self.loadConfig()
            self.configUpdated = False
            colorize_msg("Config has been reloaded", "success")
        else:
            colorize_msg("Config has not been updated", "warning")

    def do_exportToFile(self, arg):
        """
        Export the current configuration data to a file

        Examples:
            exportToFile [fileName] [optional_config_id]

        Notes:
            You can export any prior config_id from the getConfigList command by specifying it after the fileName
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        arg_list = arg.split()
        fileName = arg_list[0]

        if len(arg_list) == 1:
            json_data = self.cfgData
        else:
            config_id = arg_list[1]
            try:
                response = bytearray()
                self.g2_configmgr.getConfig(config_id, response)
                json_data = json.loads(response.decode())
            except G2Exception as err:
                colorize_msg(err, "error")
                return
        try:
            with open(fileName, "w") as fp:
                json.dump(json_data, fp, indent=4, sort_keys=True)
        except OSError as err:
            colorize_msg(err, "error")
        else:
            colorize_msg("Successfully exported!", "success")

    def do_importFromFile(self, arg):
        """
        Import the config from a file

        Examples:
            importFromFile [fileName]
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        if self.configUpdated:
            if (
                not input(
                    "\nYou have unsaved changes, are you sure you want to discard them? (y/n) "
                )
                .upper()
                .startswith("Y")
            ):
                return
        try:
            self.cfgData = json.load(open(arg, encoding="utf-8"))
        except ValueError as err:
            colorize_msg(err, "error")
        else:
            self.configUpdated = True
            colorize_msg("Successfully imported!", "success")

    # ===== settings section =====

    def do_setTheme(self, arg):
        """
        Switch terminal ANSI colors between default, light and dark

        Syntax:
            setTheme {default|light|dark}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        theme = arg.upper()
        theme, message = self.validateDomain(
            "Theme", theme, ["DEFAULT", "DARK", "LIGHT"]
        )
        if not theme:
            colorize_msg(message, "error")
            return

        Colors.set_theme(theme)

    def check_arg_for_output_format(self, output_type, arg):

        if not arg:
            return arg
        new_arg = []
        for token in arg.split():
            if token.lower() == "table" and not prettytable:
                colorize_msg(
                    "\nOutput to table ignored as prettytable not installed (pip3 install prettytable)\n",
                    "warning",
                )
                arg = arg.replace(token, "")
            elif token.lower() in ("table", "json", "jsonl"):
                if output_type == "list":
                    self.current_output_format_list = token.lower()
                else:
                    self.current_output_format_record = token.lower()
                arg = arg.replace(token, "")
            else:
                new_arg.append(token)
        return " ".join(new_arg)

    # ===== code lookups and validations =====

    def getRecord(self, table, field, value):
        # turn even single values into list to simplify code
        if not isinstance(field, list):
            field = [field]
            value = [value]

        recordList = []
        for record in self.cfgData["G2_CONFIG"][table]:
            matched = True
            for i in range(len(field)):
                if record[field[i]] != value[i]:
                    matched = False
                    break
            if matched:
                recordList.append(record)
        if recordList:
            if len(recordList) > 1:
                colorize_msg(
                    f"getRecord call for {table}, {field},{value} returned multiple rows!",
                    "warning",
                )
                for record in recordList:
                    print(record)
                print()
            return recordList[0]
        return None

    def getRecordList(self, table, field=None, value=None):
        recordList = []
        for record in self.cfgData["G2_CONFIG"][table]:
            if field and value:
                if record[field] == value:
                    recordList.append(record)
            else:
                recordList.append(record)
        return recordList

    def getDesiredValueOrNext(self, table, field, value, **kwargs):

        # turn even single values into list to simplify code
        # be sure to make last item in list the ID or order to be tested/incremented!
        if isinstance(field, list):
            if len(field) > 1:
                senior_field = field[0:-1]
                senior_value = value[0:-1]
            field = field[-1]
            value = value[-1]
        else:
            senior_field = []
            senior_value = []

        desired_id = value
        id_taken = False
        last_id = kwargs.get("seed_order", 0)
        for record in self.cfgData["G2_CONFIG"][table]:
            senior_key_match = True
            for i in range(len(senior_field)):
                if record[senior_field[i]] != senior_value[i]:
                    senior_key_match = False
                    break
            if senior_key_match:
                matched = record[field] == value
                if matched:
                    id_taken = True
                if record[field] > last_id:
                    last_id = record[field]
        return desired_id if desired_id > 0 and not id_taken else last_id + 1

    def lookupDatasource(self, dataSource):
        dsrcRecord = self.getRecord("CFG_DSRC", "DSRC_CODE", dataSource)
        if dsrcRecord:
            return dsrcRecord, f'Data source "{dataSource}" already exists'
        return None, f'Data source "{dataSource}" not found'

    def lookupFeature(self, feature):
        ftypeRecord = self.getRecord("CFG_FTYPE", "FTYPE_CODE", feature)
        if ftypeRecord:
            return ftypeRecord, f'Feature "{feature}" already exists'
        return None, f'Feature "{feature}" not found'

    def lookupElement(self, element):
        felemRecord = self.getRecord("CFG_FELEM", "FELEM_CODE", element)
        if felemRecord:
            return felemRecord, f'Element "{element}" already exists'
        return None, f'Element "{element}" not found'

    def lookupFeatureElement(self, feature, element):
        ftypeRecord, error_text = self.lookupFeature(feature)
        if not ftypeRecord:
            return None, error_text
        else:
            felemRecord = self.getRecord("CFG_FELEM", "FELEM_CODE", element)
            if felemRecord:
                fbomRecord = self.getRecord(
                    "CFG_FBOM",
                    ["FTYPE_ID", "FELEM_ID"],
                    [ftypeRecord["FTYPE_ID"], felemRecord["FELEM_ID"]],
                )
                if fbomRecord:
                    return (
                        fbomRecord,
                        f'"{element}" is already an element of feature "{feature}"',
                    )
            return (
                None,
                f'{element} is not an element of {feature} (use command "getFeature {feature}" to see its elements)',
            )

    def lookupFeatureClass(self, featureClass):
        fclassRecord = self.getRecord("CFG_FCLASS", "FCLASS_CODE", featureClass)
        if fclassRecord:
            return fclassRecord, f'Feature class "{featureClass}" exists"'
        else:
            return (
                False,
                f'Feature class "{featureClass}" not found (use command "listReferenceCodes featureClass" to see the list)',
            )

    def lookupBehaviorCode(self, behaviorCode):
        if behaviorCode in self.valid_behavior_codes:
            return (
                parseFeatureBehavior(behaviorCode),
                f'Behavior code "{behaviorCode}" exists"',
            )
        else:
            return (
                False,
                f'Behavior code "{behaviorCode}" not found (use command "listReferenceCodes behaviorCodes" to see the list)',
            )

    def lookupStandardizeFunction(self, standardizeFunction):
        funcRecord = self.getRecord("CFG_SFUNC", "SFUNC_CODE", standardizeFunction)
        if funcRecord:
            return funcRecord, f'Standardize function "{standardizeFunction}" exists"'
        else:
            return (
                False,
                f'Standardize function "{standardizeFunction}" not found (use command "listStandardizeFunctions" to see the list)',
            )

    def lookupExpressionFunction(self, expressionFunction):
        funcRecord = self.getRecord("CFG_EFUNC", "EFUNC_CODE", expressionFunction)
        if funcRecord:
            return funcRecord, f'Expression function "{expressionFunction}" exists"'
        else:
            return (
                False,
                f'Expression function "{expressionFunction}" not found (use command "listExpressionFunctions" to see the list)',
            )

    def lookupComparisonFunction(self, comparisonFunction):
        funcRecord = self.getRecord("CFG_CFUNC", "CFUNC_CODE", comparisonFunction)
        if funcRecord:
            return funcRecord, f'Comparison function "{comparisonFunction}" exists"'
        else:
            return (
                False,
                f'Comparison function "{comparisonFunction}" not found (use command "listComparisonFunctions" to see the list)',
            )

    def lookupDistinctFunction(self, distinctFunction):
        funcRecord = self.getRecord("CFG_DFUNC", "DFUNC_CODE", distinctFunction)
        if funcRecord:
            return funcRecord, f'Distinct function "{distinctFunction}" exists"'
        else:
            return (
                False,
                f'Distinct function "{distinctFunction}" not found (use command "listDistinctFunctions" to see the list)',
            )

    def validateDomain(self, attr, value, domain_list):
        if not value:
            return domain_list[0], f"{attr} defaulted to {domain_list[0]}"
        if value in domain_list:
            return value, f"{attr} value is valid!"
        else:
            return False, f"{attr} value must be in {json.dumps(domain_list)}"

    def lookupAttribute(self, attribute):
        attrRecord = self.getRecord("CFG_ATTR", "ATTR_CODE", attribute)
        if attrRecord:
            return attrRecord, f'Attribute "{attribute}" already exists!'
        return None, f'Attribute "{attribute}" not found!'

    def lookupFragment(self, lookup_value):
        if isinstance(lookup_value, int):
            erfragRecord = self.getRecord("CFG_ERFRAG", "ERFRAG_ID", lookup_value)
        else:
            erfragRecord = self.getRecord("CFG_ERFRAG", "ERFRAG_CODE", lookup_value)
        if erfragRecord:
            return erfragRecord, f'Fragment "{lookup_value}" already exists!'
        return None, f'Fragment "{lookup_value}" not found!'

    def lookupRule(self, lookup_value):
        if isinstance(lookup_value, int):
            erruleRecord = self.getRecord("CFG_ERRULE", "ERRULE_ID", lookup_value)
        else:
            erruleRecord = self.getRecord("CFG_ERRULE", "ERRULE_CODE", lookup_value)
        if erruleRecord:
            return erruleRecord, f"Rule {lookup_value} already exists!"
        return None, f"Rule {lookup_value} not found!"

    def lookupGenericPlan(self, plan):
        gplanRecord = self.getRecord("CFG_GPLAN", "GPLAN_CODE", plan)
        if gplanRecord:
            return gplanRecord, f'Plan "{plan}" already exists'
        return None, f'Plan "{plan}" not found'

    def validate_parms(self, parm_dict, required_list):
        for attr_name in parm_dict:
            attr_value = parm_dict[attr_name]
            if attr_value and self.json_attr_types.get(attr_name):
                data_type = self.json_attr_types.get(attr_name).split("|")[0]
                max_width = (
                    int(self.json_attr_types.get(attr_name).split("|")[1])
                    if "|" in self.json_attr_types.get(attr_name)
                    else 0
                )
                if data_type == "integer":
                    if not isinstance(attr_value, int):
                        if attr_value.isdigit():
                            parm_dict[attr_name] = int(parm_dict[attr_name])
                        else:
                            raise ValueError(f"{attr_name} must be an integer")
                else:
                    if not isinstance(attr_value, str):
                        raise ValueError(f"{attr_name} must be a string")
                    if max_width and len(attr_value) > max_width:
                        raise ValueError(
                            f"{attr_name} must be less than {max_width} characters"
                        )

        missing_list = []
        for attr in required_list:
            if attr not in parm_dict:
                missing_list.append(attr)
        if missing_list:
            raise ValueError(
                f"{', '.join(missing_list)} {'is' if len(missing_list) == 1 else 'are'} required"
            )

    def settable_parms(self, old_parm_data, set_parm_data, settable_parm_list):
        new_parm_data = dict(old_parm_data)
        errors = []
        update_cnt = 0
        for parm in set_parm_data:
            if parm not in old_parm_data:
                errors.append(f"{parm} is not valid for this record")
            elif set_parm_data[parm] != new_parm_data[parm]:
                if parm.upper() not in settable_parm_list:
                    errors.append(f"{parm} cannot be changed here")
                else:
                    new_parm_data[parm] = set_parm_data[parm]
                    update_cnt += 1
        new_parm_data["update_cnt"] = update_cnt
        if errors:
            new_parm_data["errors"] = (
                "The following errors were detected:\n- " + "\n- ".join(errors)
            )
        return new_parm_data

    def id_or_code_parm(self, arg_str, int_tag, str_tag, int_field, str_field):
        if arg_str.startswith("{"):
            json_parm = dictKeysUpper(json.loads(arg_str))
        elif arg_str.isdigit():
            json_parm = {int_tag: arg_str}
        else:
            json_parm = {str_tag: arg_str}

        if json_parm.get(int_tag):
            return int(json_parm.get(int_tag)), int_field
        if json_parm.get(str_tag):
            return json_parm.get(str_tag).upper(), str_field

        raise ValueError(f"Either {int_tag} or {str_tag} must be provided")

    def update_if_different(
        self, target_record, target_counter, target_field, new_value
    ):
        if target_record[target_field] != new_value:
            target_record[target_field] = new_value
            return target_record, target_counter + 1
        return target_record, target_counter

    # ===== data Source commands =====

    def do_addDataSource(self, arg):
        """
        Register a new data source

        Syntax:
            addDataSource dataSourceCode

        Examples:
            addDataSource CUSTOMER

        Caution:
            dataSource codes will automatically be converted to upper case
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = (
                dictKeysUpper(json.loads(arg))
                if arg.startswith("{")
                else {"DATASOURCE": arg}
            )
            self.validate_parms(parmData, ["DATASOURCE"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["DATASOURCE"] = parmData["DATASOURCE"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        dsrcRecord, message = self.lookupDatasource(parmData["DATASOURCE"])
        if dsrcRecord:
            colorize_msg(message, "warning")
            return

        next_id = self.getDesiredValueOrNext(
            "CFG_DSRC", "DSRC_ID", parmData.get("ID"), seed_order=1000
        )
        if parmData.get("ID") and next_id != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return
        else:
            parmData["ID"] = next_id

        parmData["RETENTIONLEVEL"], message = self.validateDomain(
            "Retention level",
            parmData.get("RETENTIONLEVEL", "Remember"),
            ["Remember", "Forget"],
        )
        if not parmData["RETENTIONLEVEL"]:
            colorize_msg(message, "error")
            return

        parmData["CONVERSATIONAL"], message = self.validateDomain(
            "Conversational", parmData.get("CONVERSATIONAL", "No"), ["Yes", "No"]
        )
        if not parmData["CONVERSATIONAL"]:
            colorize_msg(message, "error")
            return

        newRecord = {}
        newRecord["DSRC_ID"] = parmData["ID"]
        newRecord["DSRC_CODE"] = parmData["DATASOURCE"]
        newRecord["DSRC_DESC"] = parmData["DATASOURCE"]
        newRecord["DSRC_RELY"] = parmData.get("RELIABILITY", 1)
        newRecord["RETENTION_LEVEL"] = parmData["RETENTIONLEVEL"]
        newRecord["CONVERSATIONAL"] = parmData["CONVERSATIONAL"]
        self.cfgData["G2_CONFIG"]["CFG_DSRC"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Data source successfully added!", "success")

    def do_listDataSources(self, arg):
        """
        Returns the list of registered data sources

        Syntax:
            listDataSources [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for dsrcRecord in sorted(
            self.getRecordList("CFG_DSRC"), key=lambda k: k["DSRC_ID"]
        ):
            if arg and arg.lower() not in str(dsrcRecord).lower():
                continue
            json_lines.append(
                {"id": dsrcRecord["DSRC_ID"], "dataSource": dsrcRecord["DSRC_CODE"]}
            )

        self.print_json_lines(json_lines)

    def do_deleteDataSource(self, arg):
        """
        Delete an existing data source

        Syntax:
            deleteDataSource [code or id]

        Caution:
            Deleting a data source does not delete its data and you will be prevented from saving if it has data loaded!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "DATASOURCE", "DSRC_ID", "DSRC_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        dsrcRecord = self.getRecord("CFG_DSRC", searchField, searchValue)
        if not dsrcRecord:
            colorize_msg("Data source not found", "warning")
            return
        if dsrcRecord["DSRC_ID"] <= 2:
            colorize_msg(
                f"The {dsrcRecord['DSRC_CODE']} data source cannot be deleted", "error"
            )
            return

        self.cfgData["G2_CONFIG"]["CFG_DSRC"].remove(dsrcRecord)
        colorize_msg("Data source successfully deleted!", "success")
        self.configUpdated = True

    # ===== feature commands =====

    def formatFeatureJson(self, ftypeRecord):

        fclassRecord = self.getRecord(
            "CFG_FCLASS", "FCLASS_ID", ftypeRecord["FCLASS_ID"]
        )

        sfcallRecordList = self.getRecordList(
            "CFG_SFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        )
        efcallRecordList = self.getRecordList(
            "CFG_EFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        )
        cfcallRecordList = self.getRecordList(
            "CFG_CFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        )
        dfcallRecordList = self.getRecordList(
            "CFG_DFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        )

        # while rare, there can be multiple comparison, the first one can be added with the feature,
        #    the second must be added with addStandardizeCall, addExpressionCall, addComparisonCall
        sfcallRecord = (
            sorted(sfcallRecordList, key=lambda k: k["EXEC_ORDER"])[0]
            if sfcallRecordList
            else {}
        )
        efcallRecord = (
            sorted(efcallRecordList, key=lambda k: k["EXEC_ORDER"])[0]
            if efcallRecordList
            else {}
        )
        cfcallRecord = (
            sorted(cfcallRecordList, key=lambda k: k["EXEC_ORDER"])[0]
            if cfcallRecordList
            else {}
        )
        dfcallRecord = (
            sorted(dfcallRecordList, key=lambda k: k["EXEC_ORDER"])[0]
            if dfcallRecordList
            else {}
        )

        sfuncRecord = (
            self.getRecord("CFG_SFUNC", "SFUNC_ID", sfcallRecord["SFUNC_ID"])
            if sfcallRecord
            else {}
        )
        efuncRecord = (
            self.getRecord("CFG_EFUNC", "EFUNC_ID", efcallRecord["EFUNC_ID"])
            if efcallRecord
            else {}
        )
        cfuncRecord = (
            self.getRecord("CFG_CFUNC", "CFUNC_ID", cfcallRecord["CFUNC_ID"])
            if cfcallRecord
            else {}
        )
        dfuncRecord = (
            self.getRecord("CFG_DFUNC", "DFUNC_ID", dfcallRecord["DFUNC_ID"])
            if dfcallRecord
            else {}
        )

        ftypeData = {
            "id": ftypeRecord["FTYPE_ID"],
            "feature": ftypeRecord["FTYPE_CODE"],
            "class": fclassRecord["FCLASS_CODE"] if fclassRecord else "OTHER",
            "behavior": getFeatureBehavior(ftypeRecord),
            "anonymize": ftypeRecord["ANONYMIZE"],
            "candidates": ftypeRecord["USED_FOR_CAND"],
            "standardize": sfuncRecord["SFUNC_CODE"] if sfuncRecord else "",
            "expression": efuncRecord["EFUNC_CODE"] if efuncRecord else "",
            "comparison": cfuncRecord["CFUNC_CODE"] if cfuncRecord else "",
            # "distinct": dfuncRecord['DFUNC_CODE'] if dfuncRecord else '', (HIDDEN TO REDUCE CONFUSION, engineers use listDistinctCalls)
            "matchKey": ftypeRecord["SHOW_IN_MATCH_KEY"],
            "version": ftypeRecord["VERSION"],
        }
        elementList = []
        fbomRecordList = self.getRecordList(
            "CFG_FBOM", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        )
        for fbomRecord in sorted(fbomRecordList, key=lambda k: k["EXEC_ORDER"]):
            felemRecord = self.getRecord(
                "CFG_FELEM", "FELEM_ID", fbomRecord["FELEM_ID"]
            )
            if not felemRecord:
                elementList.append("ERROR: FELEM_ID %s" % fbomRecord["FELEM_ID"])
                break
            else:
                efbomRecord = efcallRecord and self.getRecord(
                    "CFG_EFBOM",
                    ["EFCALL_ID", "FTYPE_ID", "FELEM_ID"],
                    [
                        efcallRecord["EFCALL_ID"],
                        fbomRecord["FTYPE_ID"],
                        fbomRecord["FELEM_ID"],
                    ],
                )
                cfbomRecord = cfcallRecord and self.getRecord(
                    "CFG_CFBOM",
                    ["CFCALL_ID", "FTYPE_ID", "FELEM_ID"],
                    [
                        cfcallRecord["CFCALL_ID"],
                        fbomRecord["FTYPE_ID"],
                        fbomRecord["FELEM_ID"],
                    ],
                )
                elementRecord = {}
                elementRecord["element"] = felemRecord["FELEM_CODE"]
                elementRecord["expressed"] = (
                    "No" if not efcallRecord or not efbomRecord else "Yes"
                )
                elementRecord["compared"] = (
                    "No" if not cfcallRecord or not cfbomRecord else "Yes"
                )
                elementRecord["derived"] = fbomRecord["DERIVED"]
                elementRecord["display"] = (
                    "No" if fbomRecord["DISPLAY_LEVEL"] == 0 else "Yes"
                )
                elementList.append(elementRecord)

        ftypeData["elementList"] = elementList

        return ftypeData

    def do_addFeature(self, arg):
        """
        Add an new feature to be used for resolution

        Syntax:
            addFeature {json_configuration}

        Examples:
            see listFeatures or getFeature for examples of json configurations

        Notes:
            The best way to add a feature is via templateAdd as it adds both the feature and its attributes.
            If you add a feature manually, you will also have to manually add attributes for it!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            if (
                "ELEMENTLIST" not in parmData
                or len(parmData["ELEMENTLIST"]) == 0
                or not isinstance(parmData["ELEMENTLIST"], list)
            ):
                raise ValueError("elementList is required")
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
        if ftypeRecord:
            colorize_msg(message, "warning")
            return

        next_id = self.getDesiredValueOrNext(
            "CFG_FTYPE", "FTYPE_ID", parmData.get("ID"), seed_order=1000
        )
        if parmData.get("ID") and next_id != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return
        else:
            parmData["ID"] = next_id

        ftypeID = parmData["ID"]
        parmData["CLASS"] = parmData.get("CLASS", "OTHER").upper()
        parmData["BEHAVIOR"] = parmData.get("BEHAVIOR", "FM").upper()

        parmData["CANDIDATES"], message = self.validateDomain(
            "Candidates", parmData.get("CANDIDATES", "No"), ["Yes", "No"]
        )
        if not parmData["CANDIDATES"]:
            colorize_msg(message, "error")
            return

        parmData["ANONYMIZE"], message = self.validateDomain(
            "Anonymize", parmData.get("ANONYMIZE", "No"), ["Yes", "No"]
        )
        if not parmData["ANONYMIZE"]:
            colorize_msg(message, "error")
            return

        parmData["DERIVED"], message = self.validateDomain(
            "Derived", parmData.get("DERIVED", "No"), ["Yes", "No"]
        )
        if not parmData["DERIVED"]:
            colorize_msg(message, "error")
            return

        parmData["HISTORY"], message = self.validateDomain(
            "History", parmData.get("HISTORY", "Yes"), ["Yes", "No"]
        )
        if not parmData["HISTORY"]:
            colorize_msg(message, "error")
            return

        matchKeyDefault = "Yes" if parmData.get("COMPARISON") else "No"
        parmData["MATCHKEY"], message = self.validateDomain(
            "MatchKey",
            parmData.get("MATCHKEY", matchKeyDefault),
            ["Yes", "No", "Confirm", "Denial"],
        )
        if not parmData["MATCHKEY"]:
            colorize_msg(message, "error")
            return

        behaviorData, message = self.lookupBehaviorCode(parmData["BEHAVIOR"])
        if not behaviorData:
            colorize_msg(message, "error")
            return

        fclassRecord, message = self.lookupFeatureClass(parmData["CLASS"])
        if not fclassRecord:
            colorize_msg(message, "error")
            return
        fclassID = fclassRecord["FCLASS_ID"]

        sfuncID = 0
        if parmData.get("STANDARDIZE"):
            sfuncRecord, message = self.lookupStandardizeFunction(
                parmData["STANDARDIZE"]
            )
            if not sfuncRecord:
                colorize_msg(message, "error")
                return
            sfuncID = sfuncRecord["SFUNC_ID"]

        efuncID = 0
        if parmData.get("EXPRESSION"):
            efuncRecord, message = self.lookupExpressionFunction(parmData["EXPRESSION"])
            if not efuncRecord:
                colorize_msg(message, "error")
                return
            efuncID = efuncRecord["EFUNC_ID"]

        cfuncID = 0
        if parmData.get("COMPARISON"):
            cfuncRecord, message = self.lookupComparisonFunction(parmData["COMPARISON"])
            if not cfuncRecord:
                colorize_msg(message, "error")
                return
            cfuncID = cfuncRecord["CFUNC_ID"]

        # ensure elements going to express or compare routines
        if efuncID > 0 or cfuncID > 0:
            expressedCnt = comparedCnt = 0
            for element in parmData["ELEMENTLIST"]:
                if type(element) == dict:
                    element = dictKeysUpper(element)
                    if "EXPRESSED" in element and element["EXPRESSED"].upper() == "YES":
                        expressedCnt += 1
                    if "COMPARED" in element and element["COMPARED"].upper() == "YES":
                        comparedCnt += 1
            if efuncID > 0 and expressedCnt == 0:
                colorize_msg(
                    'No elements marked "expressed" for expression routine', "error"
                )
                return
            if cfuncID > 0 and comparedCnt == 0:
                colorize_msg(
                    'No elements marked "compared" for comparison routine', "error"
                )
                return

        # insert the feature
        newRecord = {}
        newRecord["FTYPE_ID"] = int(ftypeID)
        newRecord["FTYPE_CODE"] = parmData["FEATURE"]
        newRecord["FTYPE_DESC"] = parmData["FEATURE"]
        newRecord["FCLASS_ID"] = fclassID
        newRecord["FTYPE_FREQ"] = behaviorData["FREQUENCY"]
        newRecord["FTYPE_EXCL"] = behaviorData["EXCLUSIVITY"]
        newRecord["FTYPE_STAB"] = behaviorData["STABILITY"]
        newRecord["ANONYMIZE"] = parmData["ANONYMIZE"]
        newRecord["DERIVED"] = parmData["DERIVED"]
        newRecord["USED_FOR_CAND"] = parmData["CANDIDATES"]
        newRecord["SHOW_IN_MATCH_KEY"] = parmData.get("MATCHKEY", "Yes")
        # somewhat hidden fields in case an engineer wants to specify them
        newRecord["PERSIST_HISTORY"] = parmData.get("HISTORY", "Yes")
        newRecord["DERIVATION"] = parmData.get("DERIVATION")
        newRecord["VERSION"] = parmData.get("VERSION", 1)
        newRecord["RTYPE_ID"] = parmData.get("RTYPEID", 0)

        self.cfgData["G2_CONFIG"]["CFG_FTYPE"].append(newRecord)

        # add the standardize call
        sfcallID = 0
        if sfuncID > 0:
            sfcallID = self.getDesiredValueOrNext(
                "CFG_SFCALL", "SFCALL_ID", 0, seed_order=1000
            )
            newRecord = {}
            newRecord["SFCALL_ID"] = sfcallID
            newRecord["SFUNC_ID"] = sfuncID
            newRecord["EXEC_ORDER"] = 1
            newRecord["FTYPE_ID"] = ftypeID
            newRecord["FELEM_ID"] = -1
            self.cfgData["G2_CONFIG"]["CFG_SFCALL"].append(newRecord)

        # add the distinct value call (NOT SUPPORTED THROUGH HERE YET)
        dfcallID = 0
        dfuncID = 0
        if dfuncID > 0:
            dfcallID = self.getDesiredValueOrNext(
                "CFG_DFCALL", "DFCALL_ID", 0, seed_order=1000
            )
            newRecord = {}
            newRecord["DFCALL_ID"] = dfcallID
            newRecord["DFUNC_ID"] = dfuncID
            newRecord["EXEC_ORDER"] = 1
            newRecord["FTYPE_ID"] = ftypeID
            self.cfgData["G2_CONFIG"]["CFG_DFCALL"].append(newRecord)

        # add the expression call
        efcallID = 0
        if efuncID > 0:
            efcallID = self.getDesiredValueOrNext(
                "CFG_EFCALL", "EFCALL_ID", 0, seed_order=1000
            )
            newRecord = {}
            newRecord["EFCALL_ID"] = efcallID
            newRecord["EFUNC_ID"] = efuncID
            newRecord["EXEC_ORDER"] = 1
            newRecord["FTYPE_ID"] = ftypeID
            newRecord["FELEM_ID"] = -1
            newRecord["EFEAT_FTYPE_ID"] = -1
            newRecord["IS_VIRTUAL"] = "No"
            self.cfgData["G2_CONFIG"]["CFG_EFCALL"].append(newRecord)

        # add the comparison call
        cfcallID = 0
        if cfuncID > 0:
            cfcallID = self.getDesiredValueOrNext(
                "CFG_CFCALL", "CFCALL_ID", 0, seed_order=1000
            )
            newRecord = {}
            newRecord["CFCALL_ID"] = cfcallID
            newRecord["CFUNC_ID"] = cfuncID
            newRecord["EXEC_ORDER"] = 1
            newRecord["FTYPE_ID"] = ftypeID
            self.cfgData["G2_CONFIG"]["CFG_CFCALL"].append(newRecord)

        fbomOrder = 0
        for element in parmData["ELEMENTLIST"]:
            fbomOrder += 1

            if type(element) == dict:
                elementRecord = dictKeysUpper(element)
                elementRecord["ELEMENT"] = elementRecord["ELEMENT"].upper()
            else:
                elementRecord = {}
                elementRecord["ELEMENT"] = element.upper()
            if "EXPRESSED" not in elementRecord:
                elementRecord["EXPRESSED"] = "No"
            if "COMPARED" not in elementRecord:
                elementRecord["COMPARED"] = "No"

            felemRecord, message = self.lookupElement(elementRecord["ELEMENT"])
            if felemRecord:
                felemID = felemRecord["FELEM_ID"]
            else:
                felemID = self.getDesiredValueOrNext(
                    "CFG_FELEM", "FELEM_ID", 0, seed_order=1000
                )
                newRecord = {}
                newRecord["FELEM_ID"] = felemID
                newRecord["FELEM_CODE"] = elementRecord["ELEMENT"]
                newRecord["FELEM_DESC"] = elementRecord["ELEMENT"]
                newRecord["DATA_TYPE"] = "string"
                newRecord["TOKENIZE"] = "No"
                self.cfgData["G2_CONFIG"]["CFG_FELEM"].append(newRecord)

            # add all elements to distinct bom if specified
            if dfcallID > 0:
                newRecord = {}
                newRecord["DFCALL_ID"] = dfcallID
                newRecord["EXEC_ORDER"] = fbomOrder
                newRecord["FTYPE_ID"] = ftypeID
                newRecord["FELEM_ID"] = felemID
                self.cfgData["G2_CONFIG"]["CFG_DFBOM"].append(newRecord)

            # add to expression bom if directed to
            if efcallID > 0 and elementRecord["EXPRESSED"].upper() == "YES":
                newRecord = {}
                newRecord["EFCALL_ID"] = efcallID
                newRecord["EXEC_ORDER"] = fbomOrder
                newRecord["FTYPE_ID"] = ftypeID
                newRecord["FELEM_ID"] = felemID
                newRecord["FELEM_REQ"] = "Yes"
                self.cfgData["G2_CONFIG"]["CFG_EFBOM"].append(newRecord)

            # add to comparison bom if directed to
            if cfcallID > 0 and elementRecord["COMPARED"].upper() == "YES":
                newRecord = {}
                newRecord["CFCALL_ID"] = cfcallID
                newRecord["EXEC_ORDER"] = fbomOrder
                newRecord["FTYPE_ID"] = ftypeID
                newRecord["FELEM_ID"] = felemID
                self.cfgData["G2_CONFIG"]["CFG_CFBOM"].append(newRecord)

            # standardize display_level to just display while maintaining backwards compatibility
            if "DISPLAY" in elementRecord:
                elementRecord["DISPLAY_LEVEL"] = (
                    1 if elementRecord["DISPLAY"].upper() == "YES" else 0
                )

            if "DERIVED" in elementRecord:
                elementRecord["DERIVED"] = (
                    "Yes" if elementRecord["DERIVED"].upper() == "YES" else "No"
                )

            # add to feature bom always
            newRecord = {}
            newRecord["FTYPE_ID"] = ftypeID
            newRecord["FELEM_ID"] = felemID
            newRecord["EXEC_ORDER"] = fbomOrder
            newRecord["DISPLAY_LEVEL"] = elementRecord.get("DISPLAY_LEVEL", 1)
            newRecord["DISPLAY_DELIM"] = elementRecord.get("DISPLAY_DELIM")
            newRecord["DERIVED"] = elementRecord.get("DERIVED", "No")

            self.cfgData["G2_CONFIG"]["CFG_FBOM"].append(newRecord)

        self.configUpdated = True
        colorize_msg("Feature successfully added!", "success")

    def do_setFeature(self, arg):
        """
        Sets configuration parameters for an existing feature

        Syntax:
            setFeature {partial_json_configuration}

        Examples:
            setFeature {"feature": "NAME", "candidates": "Yes"}

        Caution:
            - Not everything about a feature can be set here. Some changes will require a delete and re-add of the
              feature. For instance, you cannot change a feature's ID or its list of elements.
            - Standardize, expression and comparison routines cannot be changed here.  However, you can
              use their call commands to make changes. e.g, deleteExpressionCall, addExpressionCall, etc.
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE"])
            parmData["FEATURE"] = parmData["FEATURE"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        old_ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
        if not old_ftypeRecord:
            colorize_msg(message, "warning")
            return

        ftypeRecord = dict(old_ftypeRecord)  # must use dict to create a new instance
        update_cnt = 0
        error_cnt = 0
        for parmCode in parmData:
            if parmCode == "FEATURE":
                continue
            if parmCode == "CANDIDATES":
                parmData["CANDIDATES"], message = self.validateDomain(
                    "Candidates", parmData.get("CANDIDATES", "No"), ["Yes", "No"]
                )
                if not parmData["CANDIDATES"]:
                    colorize_msg(message, "error")
                    error_cnt += 1
                else:
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord, update_cnt, "USED_FOR_CAND", parmData["CANDIDATES"]
                    )

            elif parmCode == "ANONYMIZE":
                parmData["ANONYMIZE"], message = self.validateDomain(
                    "Anonymize", parmData.get("ANONYMIZE", "No"), ["Yes", "No"]
                )
                if not parmData["ANONYMIZE"]:
                    colorize_msg(message, "error")
                    error_cnt += 1
                else:
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord, update_cnt, "ANONYMIZE", parmData["ANONYMIZE"]
                    )

            elif parmCode == "DERIVED":
                parmData["DERIVED"], message = self.validateDomain(
                    "Derived", parmData.get("DERIVED", "No"), ["Yes", "No"]
                )
                if not parmData["DERIVED"]:
                    colorize_msg(message, "error")
                    error_cnt += 1
                else:
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord, update_cnt, "DERIVED", parmData["DERIVED"]
                    )

            elif parmCode == "HISTORY":
                parmData["HISTORY"], message = self.validateDomain(
                    "History", parmData.get("HISTORY", "Yes"), ["Yes", "No"]
                )
                if not parmData["HISTORY"]:
                    colorize_msg(message, "error")
                    error_cnt += 1
                else:
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord, update_cnt, "HISTORY", parmData["HISTORY"]
                    )

            elif parmCode == "MATCHKEY":
                matchKeyDefault = "Yes" if parmData.get("COMPARISON") else "No"
                parmData["MATCHKEY"], message = self.validateDomain(
                    "MatchKey",
                    parmData.get("MATCHKEY", matchKeyDefault),
                    ["Yes", "No", "Confirm", "Denial"],
                )
                if not parmData["MATCHKEY"]:
                    colorize_msg(message, "error")
                    error_cnt += 1
                else:
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord,
                        update_cnt,
                        "SHOW_IN_MATCH_KEY",
                        parmData["MATCHKEY"],
                    )

            elif parmCode == "BEHAVIOR":
                behaviorData, message = self.lookupBehaviorCode(parmData["BEHAVIOR"])
                if not behaviorData:
                    colorize_msg(message, "error")
                    error_cnt += 1
                else:
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord, update_cnt, "FTYPE_FREQ", behaviorData["FREQUENCY"]
                    )
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord,
                        update_cnt,
                        "FTYPE_EXCL",
                        behaviorData["EXCLUSIVITY"],
                    )
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord, update_cnt, "FTYPE_STAB", behaviorData["STABILITY"]
                    )

            elif parmCode == "CLASS":
                fclassRecord, message = self.lookupFeatureClass(parmData["CLASS"])
                if not fclassRecord:
                    colorize_msg(message, "error")
                    error_cnt += 1
                else:
                    ftypeRecord, update_cnt = self.update_if_different(
                        ftypeRecord, update_cnt, "FCLASS_ID", fclassRecord["FCLASS_ID"]
                    )

            elif parmCode == "DERIVATION":
                ftypeRecord, update_cnt = self.update_if_different(
                    ftypeRecord, update_cnt, "DERIVATION", parmData["DERIVATION"]
                )

            elif parmCode == "VERSION":
                ftypeRecord, update_cnt = self.update_if_different(
                    ftypeRecord, update_cnt, "VERSION", parmData["VERSION"]
                )

            elif parmCode == "RTYPEID":
                ftypeRecord, update_cnt = self.update_if_different(
                    ftypeRecord, update_cnt, "RTYPE_ID", parmData["RTYPEID"]
                )

            elif parmCode == "ID":
                if parmData["ID"] != ftypeRecord["FTYPE_ID"]:
                    colorize_msg("Cannot change ID on features", "error")
                    error_cnt += 1
            else:
                if parmData[parmCode] != ftypeRecord.get(parmCode, ""):
                    colorize_msg(
                        f"Cannot {'set' if parmData[parmCode] else 'unset'} {parmCode} on features",
                        "error",
                    )
                    error_cnt += 1
        if error_cnt > 0:
            colorize_msg("Errors encountered, feature not updated", "error")
        elif update_cnt < 1:
            colorize_msg("No changes detected", "warning")
        else:
            self.cfgData["G2_CONFIG"]["CFG_FTYPE"].remove(old_ftypeRecord)
            self.cfgData["G2_CONFIG"]["CFG_FTYPE"].append(ftypeRecord)
            colorize_msg("Feature successfully updated!", "success")
            self.configUpdated = True

    def do_listFeatures(self, arg):
        """
        Returns the list of registered features

        Syntax:
            listFeatures [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for ftypeRecord in sorted(
            self.getRecordList("CFG_FTYPE"), key=lambda k: k["FTYPE_ID"]
        ):
            featureJson = self.formatFeatureJson(ftypeRecord)
            if arg and arg.lower() not in str(featureJson.lower()):
                continue
            json_lines.append(featureJson)

        self.print_json_lines(json_lines)

    def do_getFeature(self, arg):
        """
        Returns a specific feature's json configuration

        Syntax:
            getFeature [code or id] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "FEATURE", "FTYPE_ID", "FTYPE_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        ftypeRecord = self.getRecord("CFG_FTYPE", searchField, searchValue)
        if ftypeRecord:
            self.print_json_record(self.formatFeatureJson(ftypeRecord))
        else:
            colorize_msg("Feature not found", "error")

    def do_deleteFeature(self, arg):
        """
        Deletes a feature and its attributes

        Syntax:
            deleteFeature [code or id]

        Caution:
            Deleting a feature does not delete its data and you will be prevented from saving if it has data loaded!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "FEATURE", "FTYPE_ID", "FTYPE_CODE"
            )
        except Exception as err:
            colorize_msg(err, "error")
            return

        ftypeRecord = self.getRecord("CFG_FTYPE", searchField, searchValue)
        if not ftypeRecord:
            colorize_msg("Feature not found", "error")
            return

        if ftypeRecord["FTYPE_CODE"] in self.lockedFeatureList:
            colorize_msg(
                f"The feature {ftypeRecord['FTYPE_CODE']} cannot be deleted", "error"
            )
            return

        # also delete all supporting tables
        for fbomRecord in self.getRecordList(
            "CFG_FBOM", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        ):
            self.cfgData["G2_CONFIG"]["CFG_FBOM"].remove(fbomRecord)

        for attrRecord in self.getRecordList(
            "CFG_ATTR", "FTYPE_CODE", ftypeRecord["FTYPE_CODE"]
        ):
            self.cfgData["G2_CONFIG"]["CFG_ATTR"].remove(attrRecord)

        for sfcallRecord in self.getRecordList(
            "CFG_SFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        ):
            self.cfgData["G2_CONFIG"]["CFG_SFCALL"].remove(sfcallRecord)

        for efcallRecord in self.getRecordList(
            "CFG_EFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        ):
            for efbomRecord in self.getRecordList(
                "CFG_EFBOM", "EFCALL_ID", efcallRecord["EFCALL_ID"]
            ):
                self.cfgData["G2_CONFIG"]["CFG_EFBOM"].remove(efbomRecord)
            self.cfgData["G2_CONFIG"]["CFG_EFCALL"].remove(efcallRecord)

        for cfcallRecord in self.getRecordList(
            "CFG_CFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        ):
            for cfbomRecord in self.getRecordList(
                "CFG_CFBOM", "CFCALL_ID", cfcallRecord["CFCALL_ID"]
            ):
                self.cfgData["G2_CONFIG"]["CFG_CFBOM"].remove(cfbomRecord)
            self.cfgData["G2_CONFIG"]["CFG_CFCALL"].remove(cfcallRecord)

        for dfcallRecord in self.getRecordList(
            "CFG_DFCALL", "FTYPE_ID", ftypeRecord["FTYPE_ID"]
        ):
            for dfbomRecord in self.getRecordList(
                "CFG_DFBOM", "DFCALL_ID", dfcallRecord["DFCALL_ID"]
            ):
                self.cfgData["G2_CONFIG"]["CFG_DFBOM"].remove(dfbomRecord)
            self.cfgData["G2_CONFIG"]["CFG_DFCALL"].remove(dfcallRecord)

        self.cfgData["G2_CONFIG"]["CFG_FTYPE"].remove(ftypeRecord)
        colorize_msg("Feature successfully deleted!", "success")
        self.configUpdated = True

    # ===== feature element commands =====

    def formatElementJson(self, elementRecord):
        elementData = {
            "id": elementRecord["FELEM_ID"],
            "element": elementRecord["FELEM_CODE"],
            "datatype": elementRecord["DATA_TYPE"],
            "tokenize": elementRecord["TOKENIZE"],
        }
        return elementData

    def do_addElement(self, arg):
        """
        Adds a new element

        Syntax:
            addElement {json_configuration}

        Examples:
            see listElements for examples of json_configurations
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ELEMENT"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if self.getRecord("CFG_FELEM", "FELEM_CODE", parmData["ELEMENT"]):
            colorize_msg("Element already exists", "warning")
            return

        parmData["DATATYPE"], message = self.validateDomain(
            "DataType",
            parmData.get("DATATYPE", "string"),
            ["string", "number", "date", "datetime", "json"],
        )
        if not parmData["DATATYPE"]:
            colorize_msg(message, "error")
            return

        parmData["TOKENIZE"], message = self.validateDomain(
            "Tokenize", parmData.get("TOKENIZE", "No"), ["Yes", "No"]
        )
        if not parmData["TOKENIZE"]:
            colorize_msg(message, "error")
            return

        felemID = self.getDesiredValueOrNext(
            "CFG_FELEM", "FELEM_ID", parmData.get("ID"), seed_order=1000
        )
        if parmData.get("ID") and felemID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        newRecord = {}
        newRecord["FELEM_ID"] = felemID
        newRecord["FELEM_CODE"] = parmData["ELEMENT"]
        newRecord["FELEM_DESC"] = parmData["ELEMENT"]
        newRecord["TOKENIZE"] = parmData["TOKENIZE"]
        newRecord["DATA_TYPE"] = parmData["DATATYPE"]
        self.cfgData["G2_CONFIG"]["CFG_FELEM"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Element successfully added!", "success")

    def do_listElements(self, arg):
        """
        Returns the list of elements.

        Syntax:
            listElements [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for elementRecord in sorted(
            self.getRecordList("CFG_FELEM"), key=lambda k: k["FELEM_CODE"]
        ):
            elementJson = self.formatElementJson(elementRecord)
            if arg and arg.lower() not in str(elementJson).lower():
                continue
            json_lines.append(elementJson)

        self.print_json_lines(json_lines)

    def do_getElement(self, arg):
        """
        Returns a single element

        Syntax:
            getElement [code or id] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "ELEMENT", "FELEM_ID", "FELEM_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        elementRecord = self.getRecord("CFG_FELEM", searchField, searchValue)
        if not elementRecord:
            colorize_msg("Element does not exist", "warning")
            return
        self.print_json_record(self.formatElementJson(elementRecord))

    def do_deleteElement(self, arg):
        """
        Deletes an element

        Syntax:
            deleteElement [code or id]
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "ELEMENT", "FELEM_ID", "FELEM_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        elementRecord = self.getRecord("CFG_FELEM", searchField, searchValue)
        if not elementRecord:
            colorize_msg("Element does not exist", "warning")
            return

        fbomRecordList = self.getRecordList(
            "CFG_FBOM", "FELEM_ID", elementRecord["FELEM_ID"]
        )
        if fbomRecordList:
            featureList = ",".join(
                (
                    self.getRecord("CFG_FTYPE", "FTYPE_ID", x["FTYPE_ID"])["FTYPE_CODE"]
                    for x in fbomRecordList
                )
            )
            colorize_msg(
                f"Element linked to the following feature(s): {featureList}", "error"
            )
            return

        self.cfgData["G2_CONFIG"]["CFG_FELEM"].remove(elementRecord)
        colorize_msg("Element successfully deleted!", "success")
        self.configUpdated = True

    def do_addElementToFeature(self, arg):
        """
        Add an element to an existing feature

        Syntax:
            addElementToFeature {json_configuration}

        Example:
            addElementToFeature {"feature": "PASSPORT", "element": "STATUS", "derived": "No", "display": "Yes"}

        Notes:
            This command appends an additional element to an existing feature. The element will be added if it does not exist.
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE", "ELEMENT"])
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        ftypeRecord, message = self.lookupFeature(parmData["FEATURE"].upper())
        if not ftypeRecord:
            colorize_msg(message, "error")
            return
        ftypeID = ftypeRecord["FTYPE_ID"]

        if not self.getRecord("CFG_FELEM", "FELEM_CODE", parmData["ELEMENT"]):
            self.do_addElement(json.dumps(parmData))

        felemRecord, message = self.lookupElement(parmData["ELEMENT"])
        if (
            not felemRecord
        ):  # this should not happen as we added if not exists just above
            colorize_msg(message, "error")
            return
        felemID = felemRecord["FELEM_ID"]

        fbomRecord, message = self.lookupFeatureElement(
            parmData["FEATURE"], parmData["ELEMENT"]
        )
        if fbomRecord:
            colorize_msg(message, "warning")
            return

        parmData["DERIVED"], message = self.validateDomain(
            "Derived", parmData.get("DERIVED", "No"), ["Yes", "No"]
        )
        if not parmData["DERIVED"]:
            colorize_msg(message, "error")
            return

        if "DISPLAY_LEVEL" in parmData:
            parmData["DISPLAY"] = parmData["DISPLAY_LEVEL"]
        parmData["DISPLAY"], message = self.validateDomain(
            "Display", parmData.get("DISPLAY", "No"), ["Yes", "No"]
        )
        if not parmData["DISPLAY"]:
            colorize_msg(message, "error")
            return

        execOrder = self.getDesiredValueOrNext(
            "CFG_FBOM", "EXEC_ORDER", parmData.get("EXECORDER", 0)
        )
        if parmData.get("EXECORDER") and execOrder != parmData["EXECORDER"]:
            colorize_msg(
                "The specified execution order is already taken  (remove it to assign the next available)",
                "error",
            )
            return
        else:
            parmData["EXECORDER"] = execOrder

        newRecord = {}
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["FELEM_ID"] = felemID
        newRecord["EXEC_ORDER"] = parmData["EXECORDER"]
        newRecord["DISPLAY_LEVEL"] = 0 if parmData["DISPLAY"] == "No" else "Yes"
        newRecord["DISPLAY_DELIM"] = parmData.get("DISPLAY_DELIM")
        newRecord["DERIVED"] = parmData["DERIVED"]
        self.cfgData["G2_CONFIG"]["CFG_FBOM"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Element successfully added to feature!", "success")

    def do_setFeatureElement(self, arg):
        """
        Sets a feature element's attributes

        Syntax:
            setFeatureElement {json_configuration}

        Example:
            setFeatureElement {"feature": "ACCT_NUM", "element": "ACCT_DOMAIN", "display": "No"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE", "ELEMENT"])
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        oldRecord, message = self.lookupFeatureElement(
            parmData["FEATURE"], parmData["ELEMENT"]
        )
        if not oldRecord:
            colorize_msg(message, "warning")
            return

        oldParmData = {
            "DERIVED": oldRecord["DERIVED"],
            "DISPLAY": "No" if oldRecord["DISPLAY_LEVEL"] == 0 else "Yes",
        }

        newRecord = dict(oldRecord)  # must use dict to create a new instance
        if parmData.get("DERIVED"):
            parmData["DERIVED"], message = self.validateDomain(
                "Derived", parmData.get("DERIVED", "No"), ["Yes", "No"]
            )
            if not parmData["DERIVED"]:
                colorize_msg(message, "error")
                return
            newRecord["DERIVED"] = parmData["DERIVED"]

        if "DISPLAY" in parmData:
            if parmData["DISPLAY"] == 1:
                parmData["DISPLAY"] = "Yes"
            elif parmData["DISPLAY"] == 0:
                parmData["DISPLAY"] = "No"
            parmData["DISPLAY"], message = self.validateDomain(
                "Display", parmData.get("DISPLAY", "No"), ["Yes", "No"]
            )
            if not parmData["DISPLAY"]:
                colorize_msg(message, "error")
                return
            newRecord["DISPLAY_LEVEL"] = 0 if parmData["DISPLAY"] == "No" else 1

        if parmData.get("DISPLAY_DELIM"):
            newRecord["DISPLAY_DELIM"] = parmData["DISPLAY_DELIM"]

        self.cfgData["G2_CONFIG"]["CFG_FBOM"].remove(oldRecord)
        self.cfgData["G2_CONFIG"]["CFG_FBOM"].append(newRecord)
        colorize_msg("Feature element successfully updated!", "success")
        self.configUpdated = True

    def do_deleteElementFromFeature(self, arg):
        """
        Delete an element from a feature

        Syntax:
            deleteElementFromFeature {json_configuration}

        Example:
            deleteElementFromFeature {"feature": "PASSPORT", "element": "STATUS"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE", "ELEMENT"])
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        fbomRecord, message = self.lookupFeatureElement(
            parmData["FEATURE"], parmData["ELEMENT"]
        )
        if not fbomRecord:
            colorize_msg(message, "warning")
            return

        self.cfgData["G2_CONFIG"]["CFG_FBOM"].remove(fbomRecord)
        colorize_msg("Element successfully deleted from feature!", "success")
        self.configUpdated = True

    # ===== attribute commands =====

    def formatAttributeJson(self, attributeRecord):
        return {
            "id": attributeRecord["ATTR_ID"],
            "attribute": attributeRecord["ATTR_CODE"],
            "class": attributeRecord["ATTR_CLASS"],
            "feature": attributeRecord["FTYPE_CODE"],
            "element": attributeRecord["FELEM_CODE"],
            "required": attributeRecord["FELEM_REQ"],
            "default": attributeRecord["DEFAULT_VALUE"],
            "advanced": attributeRecord["ADVANCED"],
            "internal": attributeRecord["INTERNAL"],
        }

    def do_addAttribute(self, arg):
        """
        Adds a new attribute and maps it to a feature element

        Syntax:
            addAttribute {json_configuration}

        Examples:
            see listAttributes or getAttribute for examples of json configurations

        Notes:
            - The best way to add an attribute is via templateAdd as it adds both the feature and its attributes.
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ATTRIBUTE", "FEATURE", "ELEMENT"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["ATTRIBUTE"] = parmData["ATTRIBUTE"].upper()
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        attrRecord, message = self.lookupAttribute(parmData["ATTRIBUTE"])
        if attrRecord:
            colorize_msg(message, "warning")
            return

        next_id = self.getDesiredValueOrNext("CFG_ATTR", "ATTR_ID", parmData.get("ID"))
        if parmData.get("ID") and next_id != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken  (remove it to assign the next available)",
                "error",
            )
            return
        else:
            parmData["ID"] = next_id

        parmData["CLASS"], message = self.validateDomain(
            "Attribute class", parmData.get("CLASS", "OTHER"), self.attributeClassList
        )
        if not parmData["CLASS"]:
            colorize_msg(message, "error")
            return

        if parmData["ELEMENT"] in (
            "<PREHASHED>",
            "USED_FROM_DT",
            "USED_THRU_DT",
            "USAGE_TYPE",
        ):
            featRecord, message = self.lookupFeature(parmData["FEATURE"])
            if not featRecord:
                colorize_msg(message, "error")
                return
        else:
            fbomRecord, message = self.lookupFeatureElement(
                parmData["FEATURE"], parmData["ELEMENT"]
            )
            if not fbomRecord:
                colorize_msg(message, "error")
                return

        parmData["REQUIRED"], message = self.validateDomain(
            "Required", parmData.get("REQUIRED", "No"), ["Yes", "No", "Any", "Desired"]
        )
        if not parmData["REQUIRED"]:
            colorize_msg(message, "error")
            return

        parmData["ADVANCED"], message = self.validateDomain(
            "Advanced", parmData.get("ADVANCED", "No"), ["Yes", "No"]
        )
        if not parmData["ADVANCED"]:
            colorize_msg(message, "error")
            return

        parmData["INTERNAL"], message = self.validateDomain(
            "Internal", parmData.get("INTERNAL", "No"), ["Yes", "No"]
        )
        if not parmData["INTERNAL"]:
            colorize_msg(message, "error")
            return

        newRecord = {}
        newRecord["ATTR_ID"] = int(parmData["ID"])
        newRecord["ATTR_CODE"] = parmData["ATTRIBUTE"]
        newRecord["ATTR_CLASS"] = parmData["CLASS"]
        newRecord["FTYPE_CODE"] = parmData["FEATURE"]
        newRecord["FELEM_CODE"] = parmData["ELEMENT"]
        newRecord["FELEM_REQ"] = parmData["REQUIRED"]
        newRecord["DEFAULT_VALUE"] = parmData.get("DEFAULT")
        newRecord["ADVANCED"] = parmData["ADVANCED"]
        newRecord["INTERNAL"] = parmData["INTERNAL"]
        self.cfgData["G2_CONFIG"]["CFG_ATTR"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Attribute successfully added!", "success")

    def do_setAttribute(self, arg):
        """
        Sets existing attribute settings

        Syntax:
            setAttribute {json_configuration}

        Example:
            setAttribute {"attribute": "ACCOUNT_NUMBER", "Advanced": "Yes"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ATTRIBUTE"])
            parmData["ATTRIBUTE"] = parmData["ATTRIBUTE"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        oldRecord, message = self.lookupAttribute(parmData["ATTRIBUTE"])
        if not oldRecord:
            colorize_msg(message, "warning")
            return

        oldParmData = dictKeysUpper(self.formatAttributeJson(oldRecord))
        newParmData = self.settable_parms(
            oldParmData, parmData, ("REQUIRED", "ADVANCED", "INTERNAL")
        )
        if newParmData.get("errors"):
            colorize_msg(newParmData["errors"], "error")
            return
        if newParmData["update_cnt"] == 0:
            colorize_msg("No changes detected", "warning")
            return

        newRecord = dict(oldRecord)  # must use dict to create a new instance
        if parmData.get("REQUIRED"):
            parmData["REQUIRED"], message = self.validateDomain(
                "Required", parmData.get("REQUIRED", "No"), ["Yes", "No"]
            )
            if not parmData["REQUIRED"]:
                colorize_msg(message, "error")
                return
            newRecord["FELEM_REQ"] = parmData["REQUIRED"]

        if parmData.get("ADVANCED"):
            parmData["ADVANCED"], message = self.validateDomain(
                "Advanced", parmData.get("ADVANCED", "No"), ["Yes", "No"]
            )
            if not parmData["ADVANCED"]:
                colorize_msg(message, "error")
                return
            newRecord["ADVANCED"] = parmData["ADVANCED"]

        if parmData.get("INTERNAL"):
            parmData["INTERNAL"], message = self.validateDomain(
                "Internal", parmData.get("INTERNAL", "No"), ["Yes", "No"]
            )
            if not parmData["INTERNAL"]:
                colorize_msg(message, "error")
                return
            newRecord["INTERNAL"] = parmData["INTERNAL"]

        self.cfgData["G2_CONFIG"]["CFG_ATTR"].remove(oldRecord)
        self.cfgData["G2_CONFIG"]["CFG_ATTR"].append(newRecord)
        colorize_msg("Attribute successfully updated!", "success")
        self.configUpdated = True

    def do_listAttributes(self, arg):
        """
        Returns the list of registered attributes

        Syntax:
            listAttributes [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for attrRecord in sorted(
            self.getRecordList("CFG_ATTR"), key=lambda k: k["ATTR_ID"]
        ):
            if arg and arg.lower() not in str(attrRecord).lower():
                continue
            json_lines.append(self.formatAttributeJson(attrRecord))

        self.print_json_lines(json_lines)

    def do_getAttribute(self, arg):
        """
        Returns a specific attribute's json configuration

        Syntax:
            getAttribute [code or id] [table|json|jsonl]

        Notes:
            If you specify a valid feature, all of its attributes will be displayed
                try: getAttribute PASSPORT
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "ATTRIBUTE", "ATTR_ID", "ATTR_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        attrRecord = self.getRecord("CFG_ATTR", searchField, searchValue)
        if attrRecord:
            self.print_json_record(self.formatAttributeJson(attrRecord))

        # hack to see if they entered a valid feature
        elif self.getRecordList("CFG_ATTR", "FTYPE_CODE", searchValue):
            self.print_json_lines(
                self.getRecordList("CFG_ATTR", "FTYPE_CODE", searchValue)
            )
        else:
            colorize_msg("Attribute not found", "error")

    def do_deleteAttribute(self, arg):
        """
        Deletes an attribute

        Syntax:
            deleteAttribute [code or id]
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "ATTRIBUTE", "ATTR_ID", "ATTR_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        attrRecord = self.getRecord("CFG_ATTR", searchField, searchValue)
        if not attrRecord:
            colorize_msg("Attribute not found", "warning")
            return

        self.cfgData["G2_CONFIG"]["CFG_ATTR"].remove(attrRecord)
        colorize_msg("Attribute successfully deleted!", "success")
        self.configUpdated = True

    # ===== template commands =====

    def do_templateAdd(self, arg):
        """
        Adds a feature and its attributes based on a template

        Syntax:
            templateAdd {"feature": "feature_name", "template": "template_code", "behavior": "optional-override", "comparison": "optional-override"}

        Examples:
            templateAdd {"feature": "customer_number", "template": "global_id"}
            templateAdd {"feature": "customer_number", "template": "global_id", "behavior": "F1E"}
            templateAdd {"feature": "customer_number", "template": "global_id", "behavior": "F1E", "comparison": "exact_comp"}

        Notes:
            Type "templateAdd List" to get a list of valid templates.\n
        """
        validTemplates = {}
        validTemplates["GLOBAL_ID"] = {
            "DESCRIPTION": "globally unique identifier (like an ssn, a credit card, or a medicare_id)",
            "BEHAVIOR": ["F1", "F1E", "F1ES", "A1", "A1E", "A1ES"],
            "CANDIDATES": ["No"],
            "STANDARDIZE": ["PARSE_ID"],
            "EXPRESSION": ["EXPRESS_ID"],
            "COMPARISON": ["ID_COMP", "EXACT_COMP"],
            "FEATURE_CLASS": "ISSUED_ID",
            "ATTRIBUTE_CLASS": "IDENTIFIER",
            "ELEMENTS": [
                {
                    "element": "ID_NUM",
                    "expressed": "No",
                    "compared": "no",
                    "display": "Yes",
                },
                {
                    "element": "ID_NUM_STD",
                    "expressed": "Yes",
                    "compared": "yes",
                    "display": "No",
                },
            ],
            "ATTRIBUTES": [
                {"attribute": "<feature>", "element": "ID_NUM", "required": "Yes"}
            ],
        }

        validTemplates["STATE_ID"] = {
            "DESCRIPTION": "state issued identifier (like a drivers license)",
            "BEHAVIOR": ["F1", "F1E", "F1ES", "A1", "A1E", "A1ES"],
            "CANDIDATES": ["No"],
            "STANDARDIZE": ["PARSE_ID"],
            "EXPRESSION": ["EXPRESS_ID"],
            "COMPARISON": ["ID_COMP"],
            "FEATURE_CLASS": "ISSUED_ID",
            "ATTRIBUTE_CLASS": "IDENTIFIER",
            "ELEMENTS": [
                {
                    "element": "ID_NUM",
                    "expressed": "No",
                    "compared": "no",
                    "display": "Yes",
                },
                {
                    "element": "STATE",
                    "expressed": "No",
                    "compared": "yes",
                    "display": "Yes",
                },
                {
                    "element": "ID_NUM_STD",
                    "expressed": "Yes",
                    "compared": "yes",
                    "display": "No",
                },
            ],
            "ATTRIBUTES": [
                {
                    "attribute": "<feature>_NUMBER",
                    "element": "ID_NUM",
                    "required": "Yes",
                },
                {"attribute": "<feature>_STATE", "element": "STATE", "required": "No"},
            ],
        }

        validTemplates["COUNTRY_ID"] = {
            "DESCRIPTION": "country issued identifier (like a passport)",
            "BEHAVIOR": ["F1", "F1E", "F1ES", "A1", "A1E", "A1ES"],
            "CANDIDATES": ["No"],
            "STANDARDIZE": ["PARSE_ID"],
            "EXPRESSION": ["EXPRESS_ID"],
            "COMPARISON": ["ID_COMP"],
            "FEATURE_CLASS": "ISSUED_ID",
            "ATTRIBUTE_CLASS": "IDENTIFIER",
            "ELEMENTS": [
                {
                    "element": "ID_NUM",
                    "expressed": "No",
                    "compared": "no",
                    "display": "Yes",
                },
                {
                    "element": "COUNTRY",
                    "expressed": "No",
                    "compared": "yes",
                    "display": "Yes",
                },
                {
                    "element": "ID_NUM_STD",
                    "expressed": "Yes",
                    "compared": "yes",
                    "display": "No",
                },
            ],
            "ATTRIBUTES": [
                {
                    "attribute": "<feature>_NUMBER",
                    "element": "ID_NUM",
                    "required": "Yes",
                },
                {
                    "attribute": "<feature>_COUNTRY",
                    "element": "COUNTRY",
                    "required": "No",
                },
            ],
        }

        if arg and arg.upper() == "LIST":
            print()
            for template in validTemplates:
                print("\t", template, "-", validTemplates[template]["DESCRIPTION"])
                print("\t\tbehaviors:", validTemplates[template]["BEHAVIOR"])
                print("\t\tcomparisons:", validTemplates[template]["COMPARISON"])
                print()
            return

        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        feature = parmData["FEATURE"].upper() if "FEATURE" in parmData else None
        template = parmData["TEMPLATE"].upper() if "TEMPLATE" in parmData else None
        behavior = parmData["BEHAVIOR"].upper() if "BEHAVIOR" in parmData else None
        comparison = (
            parmData["COMPARISON"].upper() if "COMPARISON" in parmData else None
        )

        standardize = (
            parmData["STANDARDIZE"].upper() if "STANDARDIZE" in parmData else None
        )
        expression = (
            parmData["EXPRESSION"].upper() if "EXPRESSION" in parmData else None
        )
        candidates = (
            parmData["CANDIDATES"].upper() if "CANDIDATES" in parmData else None
        )

        if not feature:
            colorize_msg("A new feature name is required", "error")
            return
        if self.getRecord("CFG_FTYPE", "FTYPE_CODE", feature):
            colorize_msg("Feature already exists", "warning")
            return

        if not template:
            colorize_msg("A valid template name is required", "error")
            return
        if template not in validTemplates:
            colorize_msg("template name supplied is not valid", "error")
            return

        if not behavior:
            behavior = validTemplates[template]["BEHAVIOR"][0]
        if behavior not in validTemplates[template]["BEHAVIOR"]:
            colorize_msg("behavior code supplied is not valid for template", "error")
            return

        if not comparison:
            comparison = validTemplates[template]["COMPARISON"][0]
        if comparison not in validTemplates[template]["COMPARISON"]:
            colorize_msg("comparison code supplied is not valid for template", "error")
            return

        if not standardize:
            standardize = validTemplates[template]["STANDARDIZE"][0]
        if standardize not in validTemplates[template]["STANDARDIZE"]:
            colorize_msg("standardize code supplied is not valid for template", "error")
            return

        if not expression:
            expression = validTemplates[template]["EXPRESSION"][0]
        if expression not in validTemplates[template]["EXPRESSION"]:
            colorize_msg("expression code supplied is not valid for template", "error")
            return

        if not candidates:
            candidates = validTemplates[template]["CANDIDATES"][0]
        if candidates not in validTemplates[template]["CANDIDATES"]:
            colorize_msg(
                "candidates setting supplied is not valid for template", "error"
            )
            return

        # values that can't be overridden
        featureClass = validTemplates[template]["FEATURE_CLASS"]
        attributeClass = validTemplates[template]["ATTRIBUTE_CLASS"]

        # exact comp corrections
        if comparison == "EXACT_COMP":
            standardize = ""
            expression = ""
            candidates = "Yes"

        # build the feature
        featureData = {
            "feature": feature,
            "behavior": behavior,
            "class": featureClass,
            "candidates": candidates,
            "standardize": standardize,
            "expression": expression,
            "comparison": comparison,
            "elementList": [],
        }
        for elementDict in validTemplates[template]["ELEMENTS"]:
            if not expression:
                elementDict["expressed"] = "No"
            if not standardize:
                if elementDict["display"] == "Yes":
                    elementDict["compared"] = "Yes"
                else:
                    elementDict["compared"] = "No"
            featureData["elementList"].append(elementDict)

        featureParm = json.dumps(featureData)
        colorize_msg(f"addFeature {featureParm}", "dim")
        self.do_addFeature(featureParm)

        # build the attributes
        for attributeDict in validTemplates[template]["ATTRIBUTES"]:
            attributeDict["attribute"] = attributeDict["attribute"].replace(
                "<feature>", feature
            )

            attributeData = {
                "attribute": attributeDict["attribute"].upper(),
                "class": attributeClass,
                "feature": feature,
                "element": attributeDict["element"].upper(),
                "required": attributeDict["required"],
            }

            attributeParm = json.dumps(attributeData)
            colorize_msg(f"addAttribute {attributeParm}", "dim")
            self.do_addAttribute(attributeParm)

        return

    # ===== call command support functions =====

    def setCallTypeTables(self, call_type):
        if call_type == "expression":
            call_table = "CFG_EFCALL"
            bom_table = "CFG_EFBOM"
            call_id_field = "EFCALL_ID"
            func_table = "CFG_EFUNC"
            func_code_field = "EFUNC_CODE"
            func_id_field = "EFUNC_ID"
        elif call_type == "comparison":
            call_table = "CFG_CFCALL"
            bom_table = "CFG_CFBOM"
            call_id_field = "CFCALL_ID"
            func_table = "CFG_CFUNC"
            func_code_field = "CFUNC_CODE"
            func_id_field = "CFUNC_ID"
        elif call_type == "distinct":
            call_table = "CFG_DFCALL"
            bom_table = "CFG_DFBOM"
            call_id_field = "DFCALL_ID"
            func_table = "CFG_DFUNC"
            func_code_field = "DFUNC_CODE"
            func_id_field = "DFUNC_ID"
        elif call_type == "standardize":
            call_table = "CFG_SFCALL"
            bom_table = None
            call_id_field = "SFCALL_ID"
            func_table = "CFG_SFUNC"
            func_code_field = "SFUNC_CODE"
            func_id_field = "SFUNC_ID"
        return (
            call_table,
            bom_table,
            call_id_field,
            func_table,
            func_code_field,
            func_id_field,
        )

    def getCallID(self, feature, call_type, function=None):
        (
            call_table,
            bom_table,
            call_id_field,
            func_table,
            func_code_field,
            func_id_field,
        ) = self.setCallTypeTables(call_type)
        ftypeRecord, message = self.lookupFeature(feature.upper())
        if not ftypeRecord:
            return 0, "Feature not found"
        if function:
            func_id = self.getRecord(func_table, func_code_field, function)[
                func_id_field
            ]
            if not func_id:
                return 0, function + " not found"
            callRecord = self.getRecord(
                call_table,
                ["FTYPE_ID", func_id_field],
                [ftypeRecord["FTYPE_ID"], func_id],
            )
        else:
            callRecordList = self.getRecordList(
                call_table, "FTYPE_ID", ftypeRecord["FTYPE_ID"]
            )
            if len(callRecordList) == 1:
                callRecord = callRecordList[0]
            elif len(callRecordList) > 1:
                return 0, "Multiple call records found for feature"
            else:
                return 0, "Call record not found"

        if not callRecord:
            return 0, "Call record not found"

        return callRecord[call_id_field], "success"

    def prepCallRecord(self, call_type, arg):

        try:
            parmData = (
                dictKeysUpper(json.loads(arg))
                if arg.startswith("{")
                else {
                    "ID" if arg.isdigit() else "FEATURE": (
                        int(arg) if arg.isdigit() else arg
                    )
                }
            )
        except Exception as err:
            return None, f"Command error: {err}"

        (
            call_table,
            bom_table,
            call_id_field,
            func_table,
            func_code_field,
            func_id_field,
        ) = self.setCallTypeTables(call_type)

        possible_message = "A feature name or call ID is required"
        if parmData.get("FEATURE") and not parmData.get("ID"):
            call_id, possible_message = self.getCallID(
                parmData["FEATURE"].upper(), call_type
            )
            if call_id:
                parmData["ID"] = call_id
        if not parmData.get("ID"):
            return None, possible_message

        callRecord = self.getRecord(call_table, call_id_field, parmData["ID"])
        if not callRecord:
            return None, f"{call_type} call ID {parmData['ID']} does not exist"
        return callRecord, "success"

    def prepCallElement(self, arg):
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["CALLTYPE", "ELEMENT"])
        except Exception as err:
            return {"error": err}

        parmData["CALLTYPE"], message = self.validateDomain(
            "Call type",
            parmData.get("CALLTYPE"),
            ["expression", "comparison", "distinct"],
        )
        if not parmData["CALLTYPE"]:
            return {"error": message}
        (
            call_table,
            bom_table,
            call_id_field,
            func_table,
            func_code_field,
            func_id_field,
        ) = self.setCallTypeTables(parmData["CALLTYPE"])

        if parmData.get("FEATURE") and not parmData.get("ID"):
            call_id, possible_message = self.getCallID(
                parmData["FEATURE"].upper(), parmData["CALLTYPE"]
            )
            if call_id:
                parmData["ID"] = call_id
        else:
            possible_message = "The call_id must be specified"

        if not parmData.get("ID"):
            # return {'error': f"The call ID number must be specified - see list{parmData['CALLTYPE'][0:1].upper() + parmData['CALLTYPE'][1:].lower()}Calls to determine"}
            return {"error": possible_message}

        callRecord = self.getRecord(call_table, call_id_field, parmData["ID"])
        if not callRecord:
            return {"error": f"Call ID {parmData['ID']} does not exist"}

        ftypeID = -1
        if parmData.get("FEATURE"):
            ftypeRecord, message = self.lookupFeature(parmData["FEATURE"].upper())
            if not ftypeRecord:
                return {"error": message}
            else:
                ftypeID = ftypeRecord["FTYPE_ID"]

        if ftypeID < 0:
            felemRecord, message = self.lookupElement(parmData["ELEMENT"])
            if not felemRecord:
                return {"error": message}
            else:
                felemID = felemRecord["FELEM_ID"]
        else:
            fbomRecord, message = self.lookupFeatureElement(
                parmData["FEATURE"], parmData["ELEMENT"]
            )
            if not fbomRecord:
                return {"error": message}
            else:
                felemID = fbomRecord["FELEM_ID"]

        required, message = self.validateDomain(
            "Required", parmData.get("REQUIRED", "No"), ["Yes", "No"]
        )
        if not required:
            return {"error": message}

        bomRecord = self.getRecord(
            bom_table,
            [call_id_field, "FTYPE_ID", "FELEM_ID"],
            [parmData["ID"], ftypeID, felemID],
        )
        callElementData = {
            "call_type": parmData["CALLTYPE"],
            "call_table": call_table,
            "bom_table": bom_table,
            "call_id_field": call_id_field,
            "call_id": parmData["ID"],
            "ftypeID": ftypeID,
            "felemID": felemID,
            "exec_order": parmData.get("EXECORDER", 0),
            "required": required,
            "bomRecord": bomRecord,
        }
        return callElementData

    def addCallElement(self, arg):
        callElementData = self.prepCallElement(arg)
        if callElementData.get("error"):
            colorize_msg(callElementData["error"], "error")
            return
        if callElementData["bomRecord"]:
            colorize_msg("Feature/element already exists for call", "warning")
            return

        execOrder = self.getDesiredValueOrNext(
            callElementData["bom_table"],
            [callElementData["call_id_field"], "EXEC_ORDER"],
            [callElementData["call_id"], callElementData.get("exec_order", 0)],
        )
        if (
            callElementData.get("exec_order")
            and execOrder != callElementData["exec_order"]
        ):
            colorize_msg(
                "The specified order is already taken (remove it to assign the next available)",
                "error",
            )
            return

        newRecord = {}
        newRecord[callElementData["call_id_field"]] = callElementData["call_id"]
        newRecord["EXEC_ORDER"] = execOrder
        newRecord["FTYPE_ID"] = callElementData["ftypeID"]
        newRecord["FELEM_ID"] = callElementData["felemID"]
        if callElementData["bom_table"] == "CFG_EFBOM":
            newRecord["FELEM_REQ"] = callElementData["required"]

        self.cfgData["G2_CONFIG"][callElementData["bom_table"]].append(newRecord)
        self.configUpdated = True
        colorize_msg(
            f"{callElementData['call_type']} call element successfully added!",
            "success",
        )

    def deleteCallElement(self, arg):
        callElementData = self.prepCallElement(arg)
        if callElementData.get("error"):
            colorize_msg(callElementData["error"], "error")
            return
        if not callElementData["bomRecord"]:
            colorize_msg("Feature/element not found for call", "warning")
            return

        self.cfgData["G2_CONFIG"][callElementData["bom_table"]].remove(
            callElementData["bomRecord"]
        )
        colorize_msg(
            f"{callElementData['call_type']} call element successfully deleted!",
            "success",
        )
        self.configUpdated = True

    # ===== standardize call commands =====

    def formatStandardizeCallJson(self, sfcallRecord):
        sfcallID = sfcallRecord["SFCALL_ID"]

        ftypeRecord1 = self.getRecord("CFG_FTYPE", "FTYPE_ID", sfcallRecord["FTYPE_ID"])
        felemRecord1 = self.getRecord("CFG_FELEM", "FELEM_ID", sfcallRecord["FELEM_ID"])
        sfuncRecord = self.getRecord("CFG_SFUNC", "SFUNC_ID", sfcallRecord["SFUNC_ID"])

        sfcallData = {}
        sfcallData["id"] = sfcallID

        if ftypeRecord1:
            sfcallData["feature"] = ftypeRecord1["FTYPE_CODE"]
        else:
            sfcallData["feature"] = "all"

        if felemRecord1:
            sfcallData["element"] = felemRecord1["FELEM_CODE"]
        else:
            sfcallData["element"] = "n/a"

        sfcallData["execOrder"] = sfcallRecord["EXEC_ORDER"]
        sfcallData["function"] = sfuncRecord["SFUNC_CODE"]

        return sfcallData

    def do_addStandardizeCall(self, arg):
        """
        Add a new standardize call

        Syntax:
            addStandardizeCall {json_configuration}

        Examples:
            see listStandardizeCalls or getStandardizeCall for examples of json_configurations
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            if not parmData.get("FUNCTION") and parmData.get("STANDARDIZE"):
                parmData["FUNCTION"] = parmData["STANDARDIZE"]
            self.validate_parms(parmData, ["FUNCTION"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["EXECORDER"] = parmData.get("EXECORDER", 0)
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        sfcallID = self.getDesiredValueOrNext(
            "CFG_SFCALL", "SFCALL_ID", parmData.get("ID"), seed_order=1000
        )
        if parmData.get("ID") and sfcallID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        ftypeID = -1
        if parmData.get("FEATURE") and parmData.get("FEATURE").upper() != "ALL":
            ftypeRecord, message = self.lookupFeature(parmData["FEATURE"].upper())
            if not ftypeRecord:
                colorize_msg(message, "error")
                return
            ftypeID = ftypeRecord["FTYPE_ID"]

        felemID = -1
        if parmData.get("ELEMENT") and parmData.get("ELEMENT").upper() != "N/A":
            felemRecord, message = self.lookupElement(parmData["ELEMENT"].upper())
            if not felemRecord:
                colorize_msg(message, "error")
                return
            felemID = felemRecord["FELEM_ID"]

        if (ftypeID > 0 and felemID > 0) or (ftypeID < 0 and felemID < 0):
            colorize_msg(
                "Either a feature or an element must be specified, but not both",
                "error",
            )
            return

        sfcallOrder = self.getDesiredValueOrNext(
            "CFG_SFCALL",
            ["FTYPE_ID", "FELEM_ID", "EXEC_ORDER"],
            [ftypeID, felemID, parmData.get("EXECORDER")],
        )
        if parmData["EXECORDER"] and sfcallOrder != parmData["EXECORDER"]:
            colorize_msg(
                "The specified execution order for the feature/element is already taken",
                "error",
            )
            return

        sfuncRecord, message = self.lookupStandardizeFunction(parmData["FUNCTION"])
        if not sfuncRecord:
            colorize_msg(message, "warning")
            return
        sfuncID = sfuncRecord["SFUNC_ID"]

        newRecord = {}
        newRecord["SFCALL_ID"] = sfcallID
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["FELEM_ID"] = felemID
        newRecord["SFUNC_ID"] = sfuncID
        newRecord["EXEC_ORDER"] = sfcallOrder
        self.cfgData["G2_CONFIG"]["CFG_SFCALL"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Standardize call successfully added!", "success")

    def do_listStandardizeCalls(self, arg):
        """
        Returns the list of standardize calls.

        Syntax:
            listStandardizeCalls [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for sfcallRecord in sorted(
            self.getRecordList("CFG_SFCALL"),
            key=lambda k: (k["FTYPE_ID"], k["EXEC_ORDER"]),
        ):
            sfcallJson = self.formatStandardizeCallJson(sfcallRecord)
            if arg and arg.lower() not in str(sfcallJson).lower():
                continue
            json_lines.append(sfcallJson)

        self.print_json_lines(json_lines)

    def do_getStandardizeCall(self, arg):
        """
        Returns a single standardization call

        Syntax:
            getStandardizeCall id or feature [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("standardize", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        self.print_json_record(self.formatStandardizeCallJson(callRecord))

    def do_deleteStandardizeCall(self, arg):
        """
        Deletes a standardize call

        Syntax:
            deleteStandardizeCall id or feature
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("standardize", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        self.cfgData["G2_CONFIG"]["CFG_SFCALL"].remove(callRecord)
        colorize_msg("Standardize call successfully deleted!", "success")
        self.configUpdated = True

    # ===== expression call commands =====

    def formatExpressionCallJson(self, efcallRecord):
        efcallID = efcallRecord["EFCALL_ID"]

        ftypeRecord1 = self.getRecord("CFG_FTYPE", "FTYPE_ID", efcallRecord["FTYPE_ID"])
        felemRecord1 = self.getRecord("CFG_FELEM", "FELEM_ID", efcallRecord["FELEM_ID"])

        efuncRecord = self.getRecord("CFG_EFUNC", "EFUNC_ID", efcallRecord["EFUNC_ID"])
        efcallData = {}
        efcallData["id"] = efcallID

        if ftypeRecord1:
            efcallData["feature"] = ftypeRecord1["FTYPE_CODE"]
        else:
            efcallData["feature"] = "all"

        if felemRecord1:
            efcallData["element"] = felemRecord1["FELEM_CODE"]
        else:
            efcallData["element"] = "n/a"

        efcallData["execOrder"] = efcallRecord["EXEC_ORDER"]
        efcallData["function"] = efuncRecord["EFUNC_CODE"]
        efcallData["isVirtual"] = efcallRecord["IS_VIRTUAL"]

        ftypeRecord2 = self.getRecord(
            "CFG_FTYPE", "FTYPE_ID", efcallRecord["EFEAT_FTYPE_ID"]
        )
        if ftypeRecord2:
            efcallData["expressionFeature"] = ftypeRecord2["FTYPE_CODE"]
        else:
            efcallData["expressionFeature"] = "n/a"

        efbomList = []
        for efbomRecord in sorted(
            self.getRecordList("CFG_EFBOM", "EFCALL_ID", efcallID),
            key=lambda k: k["EXEC_ORDER"],
        ):
            ftypeRecord3 = self.getRecord(
                "CFG_FTYPE", "FTYPE_ID", efbomRecord["FTYPE_ID"]
            )
            felemRecord3 = self.getRecord(
                "CFG_FELEM", "FELEM_ID", efbomRecord["FELEM_ID"]
            )

            efbomData = {}
            efbomData["order"] = efbomRecord["EXEC_ORDER"]
            if efbomRecord["FTYPE_ID"] == 0:
                efbomData["featureLink"] = "parent"
            elif ftypeRecord3:
                efbomData["feature"] = ftypeRecord3["FTYPE_CODE"]
            if felemRecord3:
                efbomData["element"] = felemRecord3["FELEM_CODE"]
            else:
                efbomData["element"] = str(efbomRecord["FELEM_ID"])
            efbomData["required"] = efbomRecord["FELEM_REQ"]
            efbomList.append(efbomData)
        efcallData["elementList"] = efbomList

        return efcallData

    def do_addExpressionCall(self, arg):
        """
        Add a new expression call

        Syntax:
            addExpressionCall {json_configuration}

        Examples:
            see listExpressionCalls or getExpressionCall for examples of json_configurations
        """

        # uncommon examples for testing ...
        # addExpressionCall {"element":"COUNTRY_CODE", "function":"FEAT_BUILDER", "execOrder":100, "expressionFeature":"COUNTRY_OF_ASSOCIATION", "virtual":"No","elementList": [{"element":"COUNTRY", "featureLink":"parent", "required":"No"}]}
        # addExpressionCall {"element":"COUNTRY_CODE", "function":"FEAT_BUILDER", "execOrder":101, "expressionFeature":"COUNTRY_OF_ASSOCIATION", "virtual":"No","elementList": [{"element":"COUNTRY", "feature":"ADDRESS", "required":"No"}]}

        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            if not parmData.get("FUNCTION") and parmData.get("EXPRESSION"):
                parmData["FUNCTION"] = parmData["EXPRESSION"]
            self.validate_parms(parmData, ["FUNCTION", "ELEMENTLIST"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["EXECORDER"] = parmData.get("EXECORDER", 0)
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        efcallID = self.getDesiredValueOrNext(
            "CFG_EFCALL", "EFCALL_ID", parmData.get("ID"), seed_order=1000
        )
        if parmData.get("ID") and efcallID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        ftypeID = -1
        if parmData.get("FEATURE") and parmData.get("FEATURE").upper() != "ALL":
            ftypeRecord, message = self.lookupFeature(parmData["FEATURE"].upper())
            if not ftypeRecord:
                colorize_msg(message, "error")
                return
            ftypeID = ftypeRecord["FTYPE_ID"]

        felemID = -1
        if parmData.get("ELEMENT") and parmData.get("ELEMENT").upper() != "N/A":
            felemRecord, message = self.lookupElement(parmData["ELEMENT"].upper())
            if not felemRecord:
                colorize_msg(message, "error")
                return
            felemID = felemRecord["FELEM_ID"]

        if (ftypeID > 0 and felemID > 0) or (ftypeID < 0 and felemID < 0):
            colorize_msg(
                "Either a feature or an element must be specified, but not both",
                "error",
            )
            return

        efcallOrder = self.getDesiredValueOrNext(
            "CFG_EFCALL",
            ["FTYPE_ID", "FELEM_ID", "EXEC_ORDER"],
            [ftypeID, felemID, parmData.get("EXECORDER")],
        )
        if parmData["EXECORDER"] and efcallOrder != parmData["EXECORDER"]:
            colorize_msg(
                "The specified execution order for the feature/element is already taken",
                "error",
            )
            return

        efuncRecord, message = self.lookupExpressionFunction(parmData["FUNCTION"])
        if not efuncRecord:
            colorize_msg(message, "warning")
            return
        efuncID = efuncRecord["EFUNC_ID"]

        efeatFTypeID = -1
        if (
            parmData.get("EXPRESSIONFEATURE")
            and parmData.get("EXPRESSIONFEATURE").upper() != "N/A"
        ):
            ftypeRecord2, message = self.lookupFeature(
                parmData["EXPRESSIONFEATURE"].upper()
            )
            if not ftypeRecord2:
                colorize_msg(message, "warning")
                return
            efeatFTypeID = ftypeRecord2["FTYPE_ID"]

        parmData["ISVIRTUAL"], message = self.validateDomain(
            "Is virtual",
            parmData.get("ISVIRTUAL", "No"),
            ["Yes", "No", "Any", "Desired"],
        )
        if not parmData["ISVIRTUAL"]:
            colorize_msg(message, "error")
            return

        # ensure we have valid elements
        efbomRecordList = []
        execOrder = 0
        for elementData in parmData["ELEMENTLIST"]:
            elementData = dictKeysUpper(elementData)
            execOrder += 1

            if elementData.get("FEATURELINK") == "parent":
                bom_ftypeID = 0
            else:
                bom_ftypeID = -1
                if (
                    elementData.get("FEATURE")
                    and elementData.get("FEATURE").upper() != "PARENT"
                ):
                    bom_ftypeRecord, message = self.lookupFeature(
                        elementData["FEATURE"].upper()
                    )
                    if not bom_ftypeRecord:
                        colorize_msg(message, "error")
                        return
                    else:
                        bom_ftypeID = bom_ftypeRecord["FTYPE_ID"]

            bom_felemID = -1
            if (
                elementData.get("ELEMENT")
                and elementData.get("ELEMENT").upper() != "N/A"
            ):
                if bom_ftypeID > 0:
                    bom_felemRecord, message = self.lookupFeatureElement(
                        elementData.get("FEATURE").upper(),
                        elementData["ELEMENT"].upper(),
                    )
                else:
                    bom_felemRecord, message = self.lookupElement(
                        elementData["ELEMENT"].upper()
                    )
                if not bom_felemRecord:
                    colorize_msg(message, "error")
                    return
                else:
                    bom_felemID = bom_felemRecord["FELEM_ID"]
            else:
                colorize_msg(
                    f"Element required in item {execOrder} on the element list", "error"
                )
                return

            elementData["REQUIRED"], message = self.validateDomain(
                "Element required", elementData.get("REQUIRED", "No"), ["Yes", "No"]
            )
            if not elementData["REQUIRED"]:
                colorize_msg(message, "error")
                return

            efbomRecord = {}
            efbomRecord["EFCALL_ID"] = efcallID
            efbomRecord["FTYPE_ID"] = bom_ftypeID
            efbomRecord["FELEM_ID"] = bom_felemID
            efbomRecord["EXEC_ORDER"] = execOrder
            efbomRecord["FELEM_REQ"] = elementData["REQUIRED"]
            efbomRecordList.append(efbomRecord)

        if len(efbomRecordList) == 0:
            colorize_msg("No elements were found in the elementList", "error")
            return

        newRecord = {}
        newRecord["EFCALL_ID"] = efcallID
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["FELEM_ID"] = felemID
        newRecord["EFUNC_ID"] = efuncID
        newRecord["EXEC_ORDER"] = efcallOrder
        newRecord["EFEAT_FTYPE_ID"] = efeatFTypeID
        newRecord["IS_VIRTUAL"] = parmData["ISVIRTUAL"]
        self.cfgData["G2_CONFIG"]["CFG_EFCALL"].append(newRecord)
        self.cfgData["G2_CONFIG"]["CFG_EFBOM"].extend(efbomRecordList)
        self.configUpdated = True
        colorize_msg("Expression call successfully added!", "success")

    def do_listExpressionCalls(self, arg):
        """
        Returns the list of expression calls

        Syntax:
            listExpressionCalls [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for efcallRecord in sorted(
            self.getRecordList("CFG_EFCALL"),
            key=lambda k: (k["FTYPE_ID"], k["FELEM_ID"], k["EXEC_ORDER"]),
        ):
            efcallJson = self.formatExpressionCallJson(efcallRecord)
            if arg and arg.lower() not in str(efcallJson).lower():
                continue
            json_lines.append(efcallJson)

        self.print_json_lines(json_lines)

    def do_getExpressionCall(self, arg):
        """
        Returns a single expression call

        Syntax:
            getExpressionCall id or feature [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("expression", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        self.print_json_record(self.formatExpressionCallJson(callRecord))

    def do_deleteExpressionCall(self, arg):
        """
        Deletes an expression call

        Syntax:
            deleteExpressionCall id
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("expression", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        for efbomRecord in self.getRecordList(
            "CFG_EFBOM", "EFCALL_ID", callRecord["EFCALL_ID"]
        ):
            self.cfgData["G2_CONFIG"]["CFG_EFBOM"].remove(efbomRecord)
        self.cfgData["G2_CONFIG"]["CFG_EFCALL"].remove(callRecord)
        colorize_msg("Expression call successfully deleted!", "success")
        self.configUpdated = True

    def do_addExpressionCallElement(self, arg):
        """
        Add an additional feature/element to an existing expression call

        Syntax:
            addExpressionCallElement {json_configuration}

        Examples:
            addExpressionCallElement {"id": 14, "feature": "ACCT_NUM", "element": "ACCT_DOMAIN", "required": "Yes"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        self.addCallElement(addAttributeToArg(arg, add={"callType": "expression"}))

    def do_deleteExpressionCallElement(self, arg):
        """
        Delete a feature/element from an existing expression call

        Syntax:
            deleteExpressionCallElement {json_configuration}

        Examples:
            deleteExpressionCallElement {"id": 14, "feature": "ACCT_NUM", "element": "ACCT_DOMAIN"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        self.deleteCallElement(addAttributeToArg(arg, add={"callType": "expression"}))

    # ===== comparison call commands =====

    def formatComparisonCallJson(self, cfcallRecord):
        cfcallID = cfcallRecord["CFCALL_ID"]
        ftypeRecord1 = self.getRecord("CFG_FTYPE", "FTYPE_ID", cfcallRecord["FTYPE_ID"])
        cfuncRecord = self.getRecord("CFG_CFUNC", "CFUNC_ID", cfcallRecord["CFUNC_ID"])

        cfcallData = {}
        cfcallData["id"] = cfcallID
        cfcallData["feature"] = ftypeRecord1["FTYPE_CODE"] if ftypeRecord1 else "error"
        # cfcallData['execOrder'] = cfcallRecord['EXEC_ORDER']
        cfcallData["function"] = cfuncRecord["CFUNC_CODE"] if cfuncRecord else "error"

        cfbomList = []
        for cfbomRecord in sorted(
            self.getRecordList("CFG_CFBOM", "CFCALL_ID", cfcallID),
            key=lambda k: k["EXEC_ORDER"],
        ):
            ftypeRecord3 = self.getRecord(
                "CFG_FTYPE", "FTYPE_ID", cfbomRecord["FTYPE_ID"]
            )
            felemRecord3 = self.getRecord(
                "CFG_FELEM", "FELEM_ID", cfbomRecord["FELEM_ID"]
            )
            cfbomData = {}
            cfbomData["order"] = cfbomRecord["EXEC_ORDER"]
            cfbomData["element"] = (
                felemRecord3["FELEM_CODE"] if felemRecord3 else "error"
            )
            cfbomList.append(cfbomData)
        cfcallData["elementList"] = cfbomList

        return cfcallData

    def do_addComparisonCall(self, arg):
        """
        Add a new comparison call

        Syntax:
            addComparisonCall {json_configuration}

        Examples:
            see listComparisonCalls or getComparisonCall for examples of json_configurations
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            if not parmData.get("FUNCTION") and parmData.get("COMPARISON"):
                parmData["FUNCTION"] = parmData["COMPARISON"]
            self.validate_parms(parmData, ["FEATURE", "FUNCTION", "ELEMENTLIST"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        cfcallID = self.getDesiredValueOrNext(
            "CFG_CFCALL", "CFCALL_ID", parmData.get("ID"), seed_order=1000
        )
        if parmData.get("ID") and cfcallID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
        if not ftypeRecord:
            colorize_msg(message, "error")
            return
        ftypeID = ftypeRecord["FTYPE_ID"]

        parmData["EXECORDER"] = 1
        cfcallRecord = self.getRecord("CFG_CFCALL", "FTYPE_ID", ftypeID)
        if cfcallRecord:
            colorize_msg(
                f"Comparison call for function {parmData['FEATURE']} already set",
                "warning",
            )
            return

        cfuncRecord, message = self.lookupComparisonFunction(parmData["FUNCTION"])
        if not cfuncRecord:
            colorize_msg(message, "warning")
            return
        cfuncID = cfuncRecord["CFUNC_ID"]

        # ensure we have valid elements
        cfbomRecordList = []
        execOrder = 0
        for elementData in parmData["ELEMENTLIST"]:
            elementData = dictKeysUpper(elementData)
            execOrder += 1

            bom_ftypeID = (
                ftypeID  # currently elements must belong to the calling feature
            )
            bom_felemID = -1
            if elementData.get("ELEMENT"):
                bom_felemRecord, message = self.lookupFeatureElement(
                    parmData["FEATURE"], elementData["ELEMENT"].upper()
                )
                if not bom_felemRecord:
                    colorize_msg(message, "error")
                    return
                else:
                    bom_felemID = bom_felemRecord["FELEM_ID"]
            else:
                colorize_msg(
                    f"Element required in item {execOrder} on the element list", "error"
                )
                return

            cfbomRecord = {}
            cfbomRecord["CFCALL_ID"] = cfcallID
            cfbomRecord["FTYPE_ID"] = bom_ftypeID
            cfbomRecord["FELEM_ID"] = bom_felemID
            cfbomRecord["EXEC_ORDER"] = execOrder
            cfbomRecordList.append(cfbomRecord)

        if len(cfbomRecordList) == 0:
            colorize_msg("No elements were found in the elementList", "error")
            return

        newRecord = {}
        newRecord["CFCALL_ID"] = cfcallID
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["CFUNC_ID"] = cfuncID
        newRecord["EXEC_ORDER"] = parmData["EXECORDER"]
        self.cfgData["G2_CONFIG"]["CFG_CFCALL"].append(newRecord)
        self.cfgData["G2_CONFIG"]["CFG_CFBOM"].extend(cfbomRecordList)
        self.configUpdated = True
        colorize_msg("Comparison call successfully added!", "success")

    def do_listComparisonCalls(self, arg):
        """
        Returns the list of comparison calls

        Syntax:
            listComparisonCalls [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for cfcallRecord in sorted(
            self.getRecordList("CFG_CFCALL"),
            key=lambda k: (k["FTYPE_ID"], k["EXEC_ORDER"]),
        ):
            cfcallJson = self.formatComparisonCallJson(cfcallRecord)
            if arg and arg.lower() not in str(cfcallJson).lower():
                continue
            json_lines.append(cfcallJson)

        self.print_json_lines(json_lines)

    def do_getComparisonCall(self, arg):
        """
        Returns a single comparison call

        Syntax:
            getComparisonCall id or feature [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("comparison", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        self.print_json_record(self.formatComparisonCallJson(callRecord))

    def do_deleteComparisonCall(self, arg):
        """
        Deletes a comparison call

        Syntax:
            deleteComparisonCall id
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("comparison", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        for cfbomRecord in self.getRecordList(
            "CFG_CFBOM", "CFCALL_ID", callRecord["CFCALL_ID"]
        ):
            self.cfgData["G2_CONFIG"]["CFG_CFBOM"].remove(cfbomRecord)
        self.cfgData["G2_CONFIG"]["CFG_CFCALL"].remove(callRecord)
        colorize_msg("Comparison call successfully deleted!", "success")
        self.configUpdated = True

    def do_addComparisonCallElement(self, arg):
        """
        Add an additional feature/element to an existing comparison call

        Syntax:
            addComparisonCallElement {json_configuration}

        Examples:
            addComparisonCallElement {"id": 16, "feature": "ACCT_NUM", "element": "ACCT_DOMAIN"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        self.addCallElement(addAttributeToArg(arg, add={"callType": "comparison"}))

    def do_deleteComparisonCallElement(self, arg):
        """
        Delete a feature/element from an existing comparison call

        Syntax:
            deleteComparisonCallElement {json_configuration}

        Examples:
            deleteComparisonCallElement {"id": 16, "feature": "ACCT_NUM", "element": "ACCT_DOMAIN"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        self.deleteCallElement(addAttributeToArg(arg, add={"callType": "comparison"}))

    # ===== distinct call commands =====

    def formatDistinctCallJson(self, dfcallRecord):
        dfcallID = dfcallRecord["DFCALL_ID"]
        ftypeRecord1 = self.getRecord("CFG_FTYPE", "FTYPE_ID", dfcallRecord["FTYPE_ID"])
        dfuncRecord = self.getRecord("CFG_DFUNC", "DFUNC_ID", dfcallRecord["DFUNC_ID"])

        dfcallData = {}
        dfcallData["id"] = dfcallID
        dfcallData["feature"] = ftypeRecord1["FTYPE_CODE"] if ftypeRecord1 else "error"
        # dfcallData['execOrder'] = dfcallRecord['EXEC_ORDER']
        dfcallData["function"] = dfuncRecord["DFUNC_CODE"] if dfuncRecord else "error"

        dfbomList = []
        for dfbomRecord in sorted(
            self.getRecordList("CFG_DFBOM", "DFCALL_ID", dfcallID),
            key=lambda k: k["EXEC_ORDER"],
        ):
            ftypeRecord3 = self.getRecord(
                "CFG_FTYPE", "FTYPE_ID", dfbomRecord["FTYPE_ID"]
            )
            felemRecord3 = self.getRecord(
                "CFG_FELEM", "FELEM_ID", dfbomRecord["FELEM_ID"]
            )
            cfbomData = {}
            cfbomData["order"] = dfbomRecord["EXEC_ORDER"]
            cfbomData["element"] = (
                felemRecord3["FELEM_CODE"] if felemRecord3 else "error"
            )
            dfbomList.append(cfbomData)
        dfcallData["elementList"] = dfbomList

        return dfcallData

    def do_addDistinctCall(self, arg):
        """
        Add a new distinct call

        Syntax:
            addDistinctCall {json_configuration}

        Examples:
            see listDistinctCalls or getDistinctCall for examples of json_configurations
        """

        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE", "FUNCTION", "ELEMENTLIST"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        dfcallID = self.getDesiredValueOrNext(
            "CFG_DFCALL", "DFCALL_ID", parmData.get("ID"), seed_order=1000
        )
        if parmData.get("ID") and dfcallID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
        if not ftypeRecord:
            colorize_msg(message, "error")
            return
        ftypeID = ftypeRecord["FTYPE_ID"]

        parmData["EXECORDER"] = 1
        dfcallRecord = self.getRecord("CFG_DFCALL", "FTYPE_ID", ftypeID)
        if dfcallRecord:
            colorize_msg(
                f"Distinct call for function {parmData['FEATURE']} already set",
                "warning",
            )
            return

        dfuncRecord, message = self.lookupDistinctFunction(parmData["FUNCTION"])
        if not dfuncRecord:
            colorize_msg(message, "warning")
            return
        dfuncID = dfuncRecord["DFUNC_ID"]

        # ensure we have valid elements
        dfbomRecordList = []
        execOrder = 0
        for elementData in parmData["ELEMENTLIST"]:
            elementData = dictKeysUpper(elementData)
            execOrder += 1

            bom_ftypeID = (
                ftypeID  # currently elements must belong to the calling feature
            )
            bom_felemID = -1
            if elementData.get("ELEMENT"):
                bom_felemRecord, message = self.lookupFeatureElement(
                    parmData["FEATURE"], elementData["ELEMENT"].upper()
                )
                if not bom_felemRecord:
                    colorize_msg(message, "error")
                    return
                else:
                    bom_felemID = bom_felemRecord["FELEM_ID"]
            else:
                colorize_msg(
                    f"Element required in item {execOrder} on the element list", "error"
                )
                return

            dfbomRecord = {}
            dfbomRecord["DFCALL_ID"] = dfcallID
            dfbomRecord["FTYPE_ID"] = bom_ftypeID
            dfbomRecord["FELEM_ID"] = bom_felemID
            dfbomRecord["EXEC_ORDER"] = execOrder
            dfbomRecordList.append(dfbomRecord)

        if len(dfbomRecordList) == 0:
            colorize_msg("No elements were found in the elementList", "error")
            return

        newRecord = {}
        newRecord["DFCALL_ID"] = dfcallID
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["DFUNC_ID"] = dfuncID
        newRecord["EXEC_ORDER"] = parmData["EXECORDER"]
        self.cfgData["G2_CONFIG"]["CFG_DFCALL"].append(newRecord)
        self.cfgData["G2_CONFIG"]["CFG_DFBOM"].extend(dfbomRecordList)
        self.configUpdated = True
        colorize_msg("Distinct call successfully added!", "success")

    def do_listDistinctCalls(self, arg):
        """
        Returns the list of distinct calls

        Syntax:
            listDistinctCalls [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for dfcallRecord in sorted(
            self.getRecordList("CFG_DFCALL"),
            key=lambda k: (k["FTYPE_ID"], k["EXEC_ORDER"]),
        ):
            dfcallJson = self.formatDistinctCallJson(dfcallRecord)
            if arg and arg.lower() not in str(dfcallJson).lower():
                continue
            json_lines.append(dfcallJson)

        self.print_json_lines(json_lines)

    def do_getDistinctCall(self, arg):
        """
        Returns a single distinct call

        Syntax:
            getDistinctCall id or feature [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("distinct", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        self.print_json_record(self.formatDistinctCallJson(callRecord))

    def do_deleteDistinctCall(self, arg):
        """
        Deletes a distinct call

        Syntax:
            deleteDistinctCall id
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        callRecord, message = self.prepCallRecord("distinct", arg)
        if not callRecord:
            colorize_msg(message, "error")
            return

        for dfbomRecord in self.getRecordList(
            "CFG_DFBOM", "DFCALL_ID", callRecord["DFCALL_ID"]
        ):
            self.cfgData["G2_CONFIG"]["CFG_DFBOM"].remove(dfbomRecord)
        self.cfgData["G2_CONFIG"]["CFG_DFCALL"].remove(callRecord)
        colorize_msg("Distinct call successfully deleted!", "success")
        self.configUpdated = True

    def do_addDistinctCallElement(self, arg):
        """
        Add an additional feature/element to an existing distinct call

        Syntax:
            addDistinctCallElement {json_configuration}

        Examples:
            addDistinctCallElement {"id": 16, "feature": "ACCT_NUM", "element": "ACCT_DOMAIN"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        self.addCallElement(addAttributeToArg(arg, add={"callType": "distinct"}))

    def do_deleteDistinctCallElement(self, arg):
        """
        Delete a feature/element from an existing distinct call

        Syntax:
            deleteDistinctCallElement {json_configuration}

        Examples:
            deleteDistinctCallElement {"id": 16, "feature": "ACCT_NUM", "element": "ACCT_DOMAIN"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        self.deleteCallElement(addAttributeToArg(arg, add={"callType": "distinct"}))

    # ===== convenience call functions =====

    def do_addToNamehash(self, arg):
        """
        Add an additional feature/element to the list composite name keys

        Example:
            addToNamehash {"feature": "ADDRESS", "element": "STR_NUM"}

        Notes:
            This command appends an additional feature and element to the name hasher function.
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ELEMENT"])
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        parmData["CALLTYPE"] = "expression"
        call_id, message = self.getCallID("NAME", parmData["CALLTYPE"], "NAME_HASHER")
        if not call_id:
            colorize_msg(message, "error")
            return

        parmData["ID"] = call_id
        self.addCallElement(json.dumps(parmData))

    def do_deleteFromNamehash(self, arg):
        """
        Delete a feature element from the list composite name keys

        Example:
            deleteFromNamehash {"feature": "ADDRESS", "element": "STR_NUM"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ELEMENT"])
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        parmData["CALLTYPE"] = "expression"
        call_id, message = self.getCallID("NAME", parmData["CALLTYPE"], "NAME_HASHER")
        if not call_id:
            colorize_msg(message, "error")
            return

        parmData["ID"] = call_id
        self.deleteCallElement(json.dumps(parmData))

    # ===== feature behavior overrides =====

    def formatBehaviorOverrideJson(self, behaviorRecord):
        ftypeCode = self.getRecord("CFG_FTYPE", "FTYPE_ID", behaviorRecord["FTYPE_ID"])[
            "FTYPE_CODE"
        ]

        return {
            "feature": ftypeCode,
            "usageType": behaviorRecord["UTYPE_CODE"],
            "behavior": getFeatureBehavior(behaviorRecord),
        }

    def do_listBehaviorOverrides(self, arg):
        """
        Returns the list of feature behavior overrides

        Syntax:
            listBehaviorOverrides [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for behaviorRecord in sorted(
            self.getRecordList("CFG_FBOVR"),
            key=lambda k: (k["FTYPE_ID"], k["UTYPE_CODE"]),
        ):
            behaviorJson = self.formatBehaviorOverrideJson(behaviorRecord)
            if arg and arg.lower() not in str(behaviorJson).lower():
                continue
            json_lines.append(behaviorJson)

        self.print_json_lines(json_lines)

    def do_addBehaviorOverride(self, arg):
        """
        Add a new behavior override

        Syntax:
            addBehaviorOverride {json_configuration}

        Examples:
            see listBehaviorOverrides for examples of json_configurations
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE", "USAGETYPE", "BEHAVIOR"])
            parmData["FEATURE"] = parmData["FEATURE"].upper()
            parmData["USAGETYPE"] = parmData["USAGETYPE"].upper()
            parmData["BEHAVIOR"] = parmData["BEHAVIOR"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
        if not ftypeRecord:
            colorize_msg(message, "error")
            return
        ftypeID = ftypeRecord["FTYPE_ID"]

        behaviorData, message = self.lookupBehaviorCode(parmData["BEHAVIOR"])
        if not behaviorData:
            colorize_msg(message, "error")
            return

        if self.getRecord(
            "CFG_FBOVR", ["FTYPE_ID", "UTYPE_CODE"], [ftypeID, parmData["USAGETYPE"]]
        ):
            colorize_msg("Behavior override already exists", "warning")
            return

        newRecord = {}
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["ECLASS_ID"] = 0
        newRecord["UTYPE_CODE"] = parmData["USAGETYPE"]
        newRecord["FTYPE_FREQ"] = behaviorData["FREQUENCY"]
        newRecord["FTYPE_EXCL"] = behaviorData["EXCLUSIVITY"]
        newRecord["FTYPE_STAB"] = behaviorData["STABILITY"]

        self.cfgData["G2_CONFIG"]["CFG_FBOVR"].append(newRecord)
        colorize_msg("Behavior override successfully added!", "success")
        self.configUpdated = True

    def do_deleteBehaviorOverride(self, arg):
        """
        Deletes a behavior override

        Example:
            deleteBehaviorOverride {"feature": "PHONE", "usageType": "MOBILE"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FEATURE", "USAGETYPE"])
            parmData["USAGETYPE"] = parmData["USAGETYPE"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
        if not ftypeRecord:
            colorize_msg(message, "error")
            return
        ftypeID = ftypeRecord["FTYPE_ID"]

        behaviorRecord = self.getRecord(
            "CFG_FBOVR", ["FTYPE_ID", "UTYPE_CODE"], [ftypeID, parmData["USAGETYPE"]]
        )
        if not behaviorRecord:
            colorize_msg("Behavior override does not exist", "warning")
            return

        self.cfgData["G2_CONFIG"]["CFG_FBOVR"].remove(behaviorRecord)
        colorize_msg("Behavior override successfully deleted!", "success")
        self.configUpdated = True

    # ===== generic plan commands =====

    def do_listGenericPlans(self, arg):
        """
        Returns the list of generic threshold plans

        Syntax:
            listGenericPlans [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for planRecord in sorted(
            self.getRecordList("CFG_GPLAN"), key=lambda k: k["GPLAN_ID"]
        ):
            if arg and arg.lower() not in str(planRecord).lower():
                continue
            json_lines.append(
                {
                    "id": planRecord["GPLAN_ID"],
                    "plan": planRecord["GPLAN_CODE"],
                    "description": planRecord["GPLAN_DESC"],
                }
            )

        self.print_json_lines(json_lines)

    def do_cloneGenericPlan(self, arg):
        """
        Create a new generic plan based on an existing one

        Examples:
            cloneGenericPlan {"existingPlan": "SEARCH", "newPlan": "SEARCH-EXHAUSTIVE", "description": "Exhaustive search"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["EXISTINGPLAN", "NEWPLAN"])
            parmData["EXISTINGPLAN"] = parmData["EXISTINGPLAN"].upper()
            parmData["NEWPLAN"] = parmData["NEWPLAN"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        planRecord1, message = self.lookupGenericPlan(parmData["EXISTINGPLAN"])
        if not planRecord1:
            colorize_msg(message, "warning")
            return

        planRecord2, message = self.lookupGenericPlan(parmData["NEWPLAN"])
        if planRecord2:
            colorize_msg(message, "warning")
            return

        next_id = self.getDesiredValueOrNext("CFG_GPLAN", "GPLAN_ID", 0)

        newRecord = {}
        newRecord["GPLAN_ID"] = next_id
        newRecord["GPLAN_CODE"] = parmData["NEWPLAN"]
        newRecord["GPLAN_DESC"] = parmData.get("DESCRIPTION", parmData["NEWPLAN"])

        self.cfgData["G2_CONFIG"]["CFG_GPLAN"].append(newRecord)
        for thresholdRecord in self.getRecordList(
            "CFG_GENERIC_THRESHOLD", "GPLAN_ID", planRecord1["GPLAN_ID"]
        ):
            newRecord = dict(thresholdRecord)
            newRecord["GPLAN_ID"] = next_id
            self.cfgData["G2_CONFIG"]["CFG_GENERIC_THRESHOLD"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Generic plan successfully added!", "success")

    def do_deleteGenericPlan(self, arg):
        """
        Delete an existing generic threshold plan

        Syntax:
            deleteGenericPlan [code or id]

        Caution:
            Plan IDs 1 and 2 are required by the system and cannot be deleted!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "PLAN", "GPLAN_ID", "GPLAN_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        planRecord = self.getRecord("CFG_GPLAN", searchField, searchValue)
        if not planRecord:
            colorize_msg("Generic plan not found", "warning")
            return
        if planRecord["GPLAN_ID"] <= 2:
            colorize_msg(
                f"The {planRecord['GPLAN_CODE']} plan cannot be deleted", "error"
            )
            return

        self.cfgData["G2_CONFIG"]["CFG_GPLAN"].remove(planRecord)
        for thresholdRecord in self.getRecordList(
            "CFG_GENERIC_THRESHOLD", "GPLAN_ID", planRecord["GPLAN_ID"]
        ):
            self.cfgData["G2_CONFIG"]["CFG_GENERIC_THRESHOLD"].remove(thresholdRecord)
        colorize_msg("Generic plan successfully deleted!", "success")
        self.configUpdated = True

    # ===== generic threshold commands =====

    def formatGenericThresholdJson(self, thresholdRecord):
        gplanCode = self.getRecord(
            "CFG_GPLAN", "GPLAN_ID", thresholdRecord["GPLAN_ID"]
        )["GPLAN_CODE"]
        if thresholdRecord.get("FTYPE_ID", 0) != 0:
            ftypeCode = self.getRecord(
                "CFG_FTYPE", "FTYPE_ID", thresholdRecord["FTYPE_ID"]
            )["FTYPE_CODE"]
        else:
            ftypeCode = "all"

        return {
            "plan": gplanCode,
            "behavior": thresholdRecord["BEHAVIOR"],
            "feature": ftypeCode,
            "candidateCap": thresholdRecord["CANDIDATE_CAP"],
            "scoringCap": thresholdRecord["SCORING_CAP"],
            "sendToRedo": thresholdRecord["SEND_TO_REDO"],
        }

    def do_listGenericThresholds(self, arg):
        """
        Returns the list of generic thresholds

        Syntax:
            listGenericThresholds [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for thresholdRecord in sorted(
            self.getRecordList("CFG_GENERIC_THRESHOLD"),
            key=lambda k: (
                k["GPLAN_ID"],
                self.valid_behavior_codes.index(k["BEHAVIOR"]),
            ),
        ):
            thresholdJson = self.formatGenericThresholdJson(thresholdRecord)
            if arg and arg.lower() not in str(thresholdJson).lower():
                continue
            json_lines.append(thresholdJson)

        self.print_json_lines(json_lines)

    def validateGenericThreshold(self, record):
        errorList = []

        behaviorData, message = self.lookupBehaviorCode(record["BEHAVIOR"])
        if not behaviorData:
            errorList.append(message)

        record["SENDTOREDO"], message = self.validateDomain(
            "sendToRedo", record.get("SEND_TO_REDO"), ["Yes", "No"]
        )
        if not record["SENDTOREDO"]:
            errorList.append(message)

        if not isinstance(record["CANDIDATE_CAP"], int):
            errorList.append("candidateCap must be an integer")

        if not isinstance(record["SCORING_CAP"], int):
            errorList.append("scoringCap must be an integer")

        if errorList:
            print(colorize("\nThe following errors were detected:", "bad"))
            for message in errorList:
                print(colorize(f"- {message}", "bad"))
            record = None

        return record

    def do_addGenericThreshold(self, arg):
        """
        Add a new generic threshold

        Syntax:
            addGenericThreshold {json_configuration}

        Examples:
            see listGenericThresholds for examples of json_configurations
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(
                parmData,
                ["PLAN", "BEHAVIOR", "SCORINGCAP", "CANDIDATECAP", "SENDTOREDO"],
            )
            parmData["PLAN"] = parmData["PLAN"].upper()
            parmData["BEHAVIOR"] = parmData["BEHAVIOR"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        gplanRecord, message = self.lookupGenericPlan(parmData["PLAN"])
        if not gplanRecord:
            colorize_msg(message, "error")
            return
        gplan_id = gplanRecord["GPLAN_ID"]

        ftypeID = 0
        if parmData.get("FEATURE") and parmData.get("FEATURE") != "all":
            ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
            if not ftypeRecord:
                colorize_msg(message, "error")
                return
            ftypeID = ftypeRecord["FTYPE_ID"]

        if self.getRecord(
            "CFG_GENERIC_THRESHOLD",
            ["GPLAN_ID", "BEHAVIOR", "FTYPE_ID"],
            [gplan_id, parmData["BEHAVIOR"], ftypeID],
        ):
            colorize_msg("Generic threshold already exists", "warning")
            return

        newRecord = {}
        newRecord["GPLAN_ID"] = gplan_id
        newRecord["BEHAVIOR"] = parmData["BEHAVIOR"]
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["CANDIDATE_CAP"] = parmData["CANDIDATECAP"]
        newRecord["SCORING_CAP"] = parmData["SCORINGCAP"]
        newRecord["SEND_TO_REDO"] = parmData["SENDTOREDO"]
        newRecord = self.validateGenericThreshold(newRecord)
        if not newRecord:
            return

        self.cfgData["G2_CONFIG"]["CFG_GENERIC_THRESHOLD"].append(newRecord)
        colorize_msg("Generic threshold successfully added!", "success")
        self.configUpdated = True

    def do_setGenericThreshold(self, arg):
        """
        Sets the comparison thresholds for a particular comparison threshold ID

        Syntax:
            setGenericThreshold {partial_json_configuration}

        Example:
            setGenericThreshold {"plan": "SEARCH", "feature": "all", "behavior": "NAME", "candidateCap": 500}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["PLAN", "BEHAVIOR"])
            parmData["PLAN"] = parmData["PLAN"].upper()
            parmData["BEHAVIOR"] = parmData["BEHAVIOR"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        gplanRecord, message = self.lookupGenericPlan(parmData["PLAN"])
        if not gplanRecord:
            colorize_msg(message, "error")
            return
        gplan_id = gplanRecord["GPLAN_ID"]

        ftypeID = 0
        if parmData.get("FEATURE") and parmData.get("FEATURE") != "all":
            ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
            if not ftypeRecord:
                colorize_msg(message, "error")
                return
            ftypeID = ftypeRecord["FTYPE_ID"]

        oldRecord = self.getRecord(
            "CFG_GENERIC_THRESHOLD",
            ["GPLAN_ID", "BEHAVIOR", "FTYPE_ID"],
            [gplan_id, parmData["BEHAVIOR"], ftypeID],
        )
        if not oldRecord:
            colorize_msg("Generic threshold not found", "warning")
            return

        oldParmData = dictKeysUpper(self.formatGenericThresholdJson(oldRecord))
        newParmData = self.settable_parms(
            oldParmData, parmData, ("SENDTOREDO", "CANDIDATECAP", "SCORINGCAP")
        )
        if newParmData.get("errors"):
            colorize_msg(newParmData["errors"], "error")
            return
        if newParmData["update_cnt"] == 0:
            colorize_msg("No changes detected", "warning")
            return

        print(newParmData)
        newRecord = dict(oldRecord)
        newRecord["CANDIDATE_CAP"] = newParmData.get(
            "CANDIDATECAP", newRecord["CANDIDATE_CAP"]
        )
        newRecord["SCORING_CAP"] = newParmData.get(
            "SCORINGCAP", newRecord["SCORING_CAP"]
        )
        newRecord["SEND_TO_REDO"] = newParmData.get(
            "SENDTOREDO", newRecord["SEND_TO_REDO"]
        )
        newRecord = self.validateGenericThreshold(newRecord)
        if not newRecord:
            return

        self.cfgData["G2_CONFIG"]["CFG_GENERIC_THRESHOLD"].remove(oldRecord)
        self.cfgData["G2_CONFIG"]["CFG_GENERIC_THRESHOLD"].append(newRecord)
        colorize_msg("Generic threshold successfully updated!", "success")
        self.configUpdated = True

    def do_deleteGenericThreshold(self, arg):
        """
        Deletes a generic threshold record

        Example:
            deleteGenericThreshold {"plan": "search", "feature": "all", "behavior": "NAME"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["PLAN", "BEHAVIOR"])
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        gplanRecord, message = self.lookupGenericPlan(parmData["PLAN"])
        if not gplanRecord:
            colorize_msg(message, "error")
            return
        gplan_id = gplanRecord["GPLAN_ID"]

        ftypeID = 0
        if parmData.get("FEATURE") and parmData.get("FEATURE") != "all":
            ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
            if not ftypeRecord:
                colorize_msg(message, "error")
                return
            ftypeID = ftypeRecord["FTYPE_ID"]

        oldRecord = self.getRecord(
            "CFG_GENERIC_THRESHOLD",
            ["GPLAN_ID", "BEHAVIOR", "FTYPE_ID"],
            [gplan_id, parmData["BEHAVIOR"], ftypeID],
        )
        if not oldRecord:
            colorize_msg("Generic threshold not found", "warning")
            return

        self.cfgData["G2_CONFIG"]["CFG_GENERIC_THRESHOLD"].remove(oldRecord)
        colorize_msg("Generic threshold successfully deleted!", "success")
        self.configUpdated = True

    # ===== fragment commands =====

    def formatFragmentJson(self, record):
        return {
            "id": record["ERFRAG_ID"],
            "fragment": record["ERFRAG_CODE"],
            "source": record["ERFRAG_SOURCE"],
            "depends": record["ERFRAG_DEPENDS"],
        }

    def validateFragmentSource(self, sourceString):
        # compute dependencies from source
        # example: './FRAGMENT[./SAME_NAME>0 and ./SAME_STAB>0] or ./FRAGMENT[./SAME_NAME1>0 and ./SAME_STAB1>0]'
        dependencyList = []
        startPos = sourceString.find("FRAGMENT[")
        while startPos > 0:
            fragmentString = sourceString[
                startPos : sourceString.find("]", startPos) + 1
            ]
            sourceString = sourceString.replace(fragmentString, "")
            # parse the fragment string
            currentFrag = "eof"
            fragmentChars = list(fragmentString)
            potentialErrorString = ""
            for thisChar in fragmentChars:
                potentialErrorString += thisChar
                if thisChar == "/":
                    currentFrag = ""
                elif currentFrag != "eof":
                    if thisChar in "| =><)":
                        # lookup the fragment code
                        fragRecord = self.getRecord(
                            "CFG_ERFRAG", "ERFRAG_CODE", currentFrag
                        )
                        if not fragRecord:
                            return [], f"Invalid fragment reference: {currentFrag}"
                        else:
                            dependencyList.append(str(fragRecord["ERFRAG_ID"]))
                        currentFrag = "eof"
                    else:
                        currentFrag += thisChar
            # next list of fragments
            startPos = sourceString.find("FRAGMENT[")
        return dependencyList, ""

    def do_addFragment(self, arg):
        """
        Adds a new rule fragment

        Syntax:
            addFragment {json_configuration}

        Examples:
            see listFragments or getFragment for examples of json configurations
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FRAGMENT", "SOURCE"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FRAGMENT"] = parmData["FRAGMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if self.getRecord("CFG_ERFRAG", "ERFRAG_CODE", parmData["FRAGMENT"]):
            colorize_msg("Fragment already exists", "warning")
            return

        erfragID = self.getDesiredValueOrNext(
            "CFG_ERFRAG", "ERFRAG_ID", parmData.get("ID")
        )
        if parmData.get("ID") and erfragID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        if parmData.get("DEPENDS"):
            colorize_msg(
                "Depends setting ignored as it is calculated by the system", "warning"
            )

        dependencyList, error_message = self.validateFragmentSource(parmData["SOURCE"])
        if error_message:
            colorize_msg(error_message, "error")
            return

        newRecord = {}
        newRecord["ERFRAG_ID"] = erfragID
        newRecord["ERFRAG_CODE"] = parmData["FRAGMENT"]
        newRecord["ERFRAG_DESC"] = parmData["FRAGMENT"]
        newRecord["ERFRAG_SOURCE"] = parmData["SOURCE"]
        newRecord["ERFRAG_DEPENDS"] = (
            ",".join(dependencyList) if dependencyList else None
        )
        self.cfgData["G2_CONFIG"]["CFG_ERFRAG"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Fragment successfully added!", "success")

    def do_setFragment(self, arg):
        """
        Sets configuration parameters for an existing feature

        Syntax:
            setFragment {partial_json_configuration}

        Examples:
            setFragment {"fragment": "GNR_ORG_NAME", "source": "./SCORES/NAME[./GNR_ON>=90]"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            if not parmData.get("ID") and not parmData.get("FRAGMENT"):
                raise ValueError("Either ID or FRAGMENT must be supplied")
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        oldRecord, message = self.lookupFragment(
            parmData["ID"] if parmData.get("ID") else parmData["FRAGMENT"].upper()
        )
        if not oldRecord:
            colorize_msg(message, "warning")
            return

        oldParmData = dictKeysUpper(self.formatFragmentJson(oldRecord))
        settable_parm_list = "SOURCE"
        newParmData = self.settable_parms(oldParmData, parmData, settable_parm_list)
        if newParmData.get("errors"):
            colorize_msg(newParmData["errors"], "error")
            return
        if newParmData["update_cnt"] == 0:
            colorize_msg("No changes detected", "warning")
            return

        newRecord = dict(oldRecord)  # must use dict to create a new instance
        dependencyList, error_message = self.validateFragmentSource(parmData["SOURCE"])
        if error_message:
            colorize_msg(error_message, "error")
            return

        newRecord["ERFRAG_SOURCE"] = parmData["SOURCE"]
        newRecord["ERFRAG_DEPENDS"] = (
            ",".join(dependencyList) if dependencyList else None
        )
        self.cfgData["G2_CONFIG"]["CFG_ERFRAG"].remove(oldRecord)
        self.cfgData["G2_CONFIG"]["CFG_ERFRAG"].append(newRecord)
        colorize_msg("Fragment successfully updated!", "success")
        self.configUpdated = True

    def do_listFragments(self, arg):
        """
        Returns the list of rule fragments.

        Syntax:
            listFragments [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for fragmentRecord in sorted(
            self.getRecordList("CFG_ERFRAG"), key=lambda k: k["ERFRAG_ID"]
        ):
            fragmentJson = self.formatFragmentJson(fragmentRecord)
            if arg and arg.lower() not in str(fragmentJson).lower():
                continue
            json_lines.append(fragmentJson)

        self.print_json_lines(json_lines)

    def do_getFragment(self, arg):
        """
        Returns a single rule fragment

        Syntax:
            getFragment [code or id] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "FRAGMENT", "ERFRAG_ID", "ERFRAG_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        fragmentRecord = self.getRecord("CFG_ERFRAG", searchField, searchValue)
        if not fragmentRecord:
            colorize_msg("Fragment does not exist", "warning")
            return
        self.print_json_record(self.formatFragmentJson(fragmentRecord))

    def do_deleteFragment(self, arg):
        """
        Deletes a rule fragment

        Syntax:
            deleteFragment [code or id]
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "FRAGMENT", "ERFRAG_ID", "ERFRAG_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        fragmentRecord = self.getRecord("CFG_ERFRAG", searchField, searchValue)
        if not fragmentRecord:
            colorize_msg("Fragment does not exist", "warning")
            return

        self.cfgData["G2_CONFIG"]["CFG_ERFRAG"].remove(fragmentRecord)
        colorize_msg("Fragment successfully deleted!", "success")
        self.configUpdated = True

    # ===== rule commands =====

    def formatRuleJson(self, record):
        return {
            "id": record["ERRULE_ID"],
            "rule": record["ERRULE_CODE"],
            "desc": record["ERRULE_DESC"],
            "resolve": record["RESOLVE"],
            "relate": record["RELATE"],
            "ref_score": record["REF_SCORE"],
            "fragment": record["QUAL_ERFRAG_CODE"],
            "disqualifier": record["DISQ_ERFRAG_CODE"],
            "rtype_id": record["RTYPE_ID"],
            "tier": record["ERRULE_TIER"] if record["RESOLVE"] == "Yes" else None,
        }

    def validateRule(self, record):

        erfragRecord, message = self.lookupFragment(record["QUAL_ERFRAG_CODE"])
        if not erfragRecord:
            colorize_msg(message, "error")
            return None

        if record.get("DISQ_ERFRAG_CODE"):
            dqfragRecord, message = self.lookupFragment(record["DISQ_ERFRAG_CODE"])
            if not dqfragRecord:
                colorize_msg(message, "error")
                return None

        record["RESOLVE"], message = self.validateDomain(
            "resolve", record.get("RESOLVE", "No"), ["Yes", "No"]
        )
        if not record["RESOLVE"]:
            colorize_msg(message, "error")
            return None

        record["RELATE"], message = self.validateDomain(
            "relate", record.get("RELATE", "No"), ["Yes", "No"]
        )
        if not record["RELATE"]:
            colorize_msg(message, "error")
            return None

        if record["RESOLVE"] == "Yes" and record["RELATE"] == "Yes":
            colorize_msg(
                "A rule must either resolve or relate, please set the other to No",
                "error",
            )
            return None

        tier = record.get("ERRULE_TIER")
        rtypeID = record.get("RTYPE_ID")

        if record["RESOLVE"] == "Yes":
            if not tier:
                colorize_msg(
                    "A tier matching other rules that could be considered ambiguous to this one must be specified",
                    "error",
                )
                return None

            if rtypeID != 1:
                # just do it without making them wonder
                # colorize_msg('Relationship type (RTYPE_ID) was forced to 1 for resolve rule', 'warning')
                record["RTYPE_ID"] = 1

        if record["RELATE"] == "Yes":
            # leave tier as is as they may change back to resolve and don't want to lose its original setting
            # if tier:
            #     colorize_msg('A tier is not required for relate rules', 'error')
            if rtypeID not in (2, 3, 4):
                colorize_msg(
                    "Relationship type (RTYPE_ID) must be set to either 2=Possible match or 3=Possibly related",
                    "error",
                )
                return None
        return record

    def do_addRule(self, arg):
        """
        Adds a new rule (aka principle)

        Syntax:
            addRule {json_configuration}

        Examples:
            see listRules or getRule for examples of json configurations
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(
                parmData, ["ID", "RULE", "FRAGMENT", "RESOLVE", "RELATE", "RTYPE_ID"]
            )
            parmData["RULE"] = parmData["RULE"].upper()
            parmData["FRAGMENT"] = parmData["FRAGMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if self.getRecord("CFG_ERRULE", "ERRULE_CODE", parmData["RULE"]):
            colorize_msg("Rule already exists", "warning")
            return

        erruleID = self.getDesiredValueOrNext(
            "CFG_ERRULE", "ERRULE_ID", parmData.get("ID")
        )
        if parmData.get("ID") and erruleID != parmData["ID"]:
            colorize_msg("The specified ID is already taken", "error")
            return

        newRecord = {}
        newRecord["ERRULE_ID"] = parmData["ID"]
        newRecord["ERRULE_CODE"] = parmData["RULE"]
        newRecord["ERRULE_DESC"] = parmData.get("DESC", parmData["RULE"])
        newRecord["RESOLVE"] = parmData["RESOLVE"]
        newRecord["RELATE"] = parmData["RELATE"]
        newRecord["REF_SCORE"] = parmData.get("REF_SCORE", 0)
        newRecord["RTYPE_ID"] = parmData["RTYPE_ID"]
        newRecord["QUAL_ERFRAG_CODE"] = parmData["FRAGMENT"]
        newRecord["DISQ_ERFRAG_CODE"] = parmData.get("DISQUALIFIER")
        newRecord["ERRULE_TIER"] = parmData.get("TIER")

        newRecord = self.validateRule(newRecord)
        if not newRecord:
            # colorize_msg('Rule not added', 'error')
            return

        self.cfgData["G2_CONFIG"]["CFG_ERRULE"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Rule successfully added!", "success")

    def do_setRule(self, arg):
        """
        Syntax:
            setRule {partial json configuration}

        Examples:
            setRule {"id": 111, "resolve": "No"}
            setRule {"id": 111, "relate": "Yes", "rtype_id": 2}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ID"])
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        oldRecord, message = self.lookupRule(parmData["ID"])
        if not oldRecord:
            colorize_msg(message, "warning")
            return

        oldParmData = dictKeysUpper(self.formatRuleJson(oldRecord))
        settable_parm_list = (
            "RULE",
            "DESC",
            "RESOLVE",
            "RELATE",
            "REF_SCORE",
            "RTYPE_ID",
            "FRAGMENT",
            "DISQUALIFIER",
            "TIER",
        )
        newParmData = self.settable_parms(oldParmData, parmData, settable_parm_list)
        if newParmData.get("errors"):
            colorize_msg(newParmData["errors"], "error")
            return
        if newParmData["update_cnt"] == 0:
            colorize_msg("No changes detected", "warning")
            return

        newRecord = dict(oldRecord)  # must use dict to create a new instance
        newRecord["ERRULE_CODE"] = parmData.get("RULE", newRecord["ERRULE_CODE"])
        newRecord["ERRULE_DESC"] = parmData.get("DESC", newRecord["ERRULE_DESC"])
        newRecord["RESOLVE"] = parmData.get("RESOLVE", newRecord["RESOLVE"])
        newRecord["RELATE"] = parmData.get("RELATE", newRecord["RELATE"])
        newRecord["REF_SCORE"] = parmData.get("REF_SCORE", newRecord["REF_SCORE"])
        newRecord["RTYPE_ID"] = parmData.get("RTYPE_ID", newRecord["RTYPE_ID"])
        newRecord["QUAL_ERFRAG_CODE"] = parmData.get(
            "FRAGMENT", newRecord["QUAL_ERFRAG_CODE"]
        )
        newRecord["DISQ_ERFRAG_CODE"] = parmData.get(
            "DISQUALIFIER", newRecord["DISQ_ERFRAG_CODE"]
        )
        newRecord["ERRULE_TIER"] = parmData.get("TIER", newRecord["ERRULE_TIER"])

        newRecord = self.validateRule(newRecord)
        if not newRecord:
            # colorize_msg('Rule not updated', 'error')
            return

        self.cfgData["G2_CONFIG"]["CFG_ERRULE"].remove(oldRecord)
        self.cfgData["G2_CONFIG"]["CFG_ERRULE"].append(newRecord)
        colorize_msg("Rule successfully updated!", "success")
        self.configUpdated = True

    def do_listRules(self, arg):
        """
        Returns the list of rules (aka principles)

        Syntax:
            listRules [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)

        json_lines = []
        for ruleRecord in sorted(
            self.getRecordList("CFG_ERRULE"), key=lambda k: k["ERRULE_ID"]
        ):
            ruleJson = self.formatRuleJson(ruleRecord)
            if arg and arg.lower() not in str(ruleJson).lower():
                continue
            json_lines.append(ruleJson)

        self.print_json_lines(json_lines)

    def do_getRule(self, arg):
        """
        Returns a single rule (aka principle)

        Syntax:
            getRule [code or id] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("record", arg)
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "RULE", "ERRULE_ID", "ERRULE_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        ruleRecord = self.getRecord("CFG_ERRULE", searchField, searchValue)
        if not ruleRecord:
            colorize_msg("Rule does not exist", "warning")
            return
        self.print_json_record(self.formatRuleJson(ruleRecord))

    def do_deleteRule(self, arg):
        """
        Deletes a rule (aka principle)

        Syntax:
            deleteRule [code or id]
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            searchValue, searchField = self.id_or_code_parm(
                arg, "ID", "RULE", "ERRULE_ID", "ERRULE_CODE"
            )
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        ruleRecord = self.getRecord("CFG_ERRULE", searchField, searchValue)
        if not ruleRecord:
            colorize_msg("Rule does not exist", "warning")
            return

        self.cfgData["G2_CONFIG"]["CFG_ERRULE"].remove(ruleRecord)
        colorize_msg("Rule successfully deleted!", "success")
        self.configUpdated = True

    # ===== supporting codes =====

    def do_listReferenceCodes(self, arg):
        """
        Returns the list of internal reference codes

        Syntax:
            listReferenceCodes [code_type] [table|json|jsonl]

        Notes:
            reference code types include:
                matchLevels
                behaviorCodes
                featureClasses
                attributeClasses
        """
        arg = self.check_arg_for_output_format("list", arg)
        if arg:
            arg = arg.upper()

        if not arg or arg in "MATCHLEVELS":
            json_lines = []
            for rtypeRecord in sorted(
                self.getRecordList("CFG_RTYPE"), key=lambda k: k["RTYPE_ID"]
            ):
                if arg and arg.lower() not in str(rtypeRecord).lower():
                    continue
                json_lines.append(
                    {
                        "level": rtypeRecord["RTYPE_ID"],
                        "code": rtypeRecord["RTYPE_CODE"],
                        "class": self.getRecord(
                            "CFG_RCLASS", "RCLASS_ID", rtypeRecord["RCLASS_ID"]
                        )["RCLASS_DESC"],
                    }
                )
            self.print_json_lines(json_lines, "Match Levels")

        if not arg or arg in "BEHAVIORCODES":
            json_lines = []
            for code in self.valid_behavior_codes:
                if code == "NAME":
                    desc = "Controlled behavior used only for names"
                elif code == "NONE":
                    desc = "No behavior"
                else:
                    if code.startswith("A1"):
                        desc = "Absolutely 1"
                    elif code.startswith("F1"):
                        desc = "Frequency 1"
                    elif code.startswith("FF"):
                        desc = "Frequency 1"
                    elif code.startswith("FM"):
                        desc = "Frequency many"
                    elif code.startswith("FVM"):
                        desc = "Frequency very many"
                    else:
                        desc = "unknown"
                    if "E" in code:
                        desc += ", exclusive"
                    if "S" in code:
                        desc += " and stable"
                json_lines.append({"behaviorCode": code, "behaviorDescription": desc})
            self.print_json_lines(json_lines, "Behavior Codes")

        if not arg or arg in "FEATURECLASS":
            json_lines = []
            for fclassRecord in sorted(
                self.getRecordList("CFG_FCLASS"), key=lambda k: k["FCLASS_ID"]
            ):
                json_lines.append(
                    {
                        "class": fclassRecord["FCLASS_CODE"],
                        "id": fclassRecord["FCLASS_ID"],
                    }
                )
            self.print_json_lines(json_lines, "Feature Classes")

        if not arg or arg in "ATTRIBUTECLASS":
            json_lines = []
            for attrClass in self.attributeClassList:
                json_lines.append({"attributeClass": attrClass})
            self.print_json_lines(json_lines, "Attribute Classes")

    # standardize functions

    def do_addStandardizeFunction(self, arg):
        """
        Adds a new standardize function

        Syntax:
            addStandardizeFunction {json_configuration}

        Examples:
            see listStandardizeFunctions for examples of json_configurations

        Caution:
            Adding a new function requires a plugin to be programmed!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FUNCTION"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if self.getRecord("CFG_SFUNC", "SFUNC_CODE", parmData["FUNCTION"]):
            colorize_msg("Function already exists", "warning")
            return

        sfuncID = self.getDesiredValueOrNext(
            "CFG_SFUNC", "SFUNC_ID", parmData.get("ID")
        )
        if parmData.get("ID") and sfuncID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        parmData["FUNCLIB"] = parmData.get("FUNCLIB", "g2func_lib")
        parmData["VERSION"] = parmData.get("VERSION", 1)
        parmData["CONNECTSTR"] = parmData.get("CONNECTSTR", None)
        parmData["LANGUAGE"] = parmData.get("LANGUAGE", None)
        parmData["JAVACLASSNAME"] = parmData.get("JAVACLASSNAME", None)

        newRecord = {}
        newRecord["SFUNC_ID"] = sfuncID
        newRecord["SFUNC_CODE"] = parmData["FUNCTION"]
        newRecord["SFUNC_DESC"] = parmData["FUNCTION"]
        newRecord["FUNC_LIB"] = parmData["FUNCLIB"]
        newRecord["FUNC_VER"] = parmData["VERSION"]
        newRecord["CONNECT_STR"] = parmData["CONNECTSTR"]
        newRecord["LANGUAGE"] = parmData["LANGUAGE"]
        newRecord["JAVA_CLASS_NAME"] = parmData["JAVACLASSNAME"]
        self.cfgData["G2_CONFIG"]["CFG_SFUNC"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Standardize function successfully added!", "success")

    def do_listStandardizeFunctions(self, arg):
        """
        Returns the list of standardize functions

        Syntax:
            listStandardizeFunctions [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for funcRecord in sorted(
            self.getRecordList("CFG_SFUNC"), key=lambda k: k["SFUNC_ID"]
        ):
            if arg and arg.lower() not in str(funcRecord).lower():
                continue
            json_lines.append(
                {
                    "id": funcRecord["SFUNC_ID"],
                    "function": funcRecord["SFUNC_CODE"],
                    "connectStr": funcRecord["CONNECT_STR"],
                    "language": funcRecord["LANGUAGE"],
                    "javaClassName": funcRecord["JAVA_CLASS_NAME"],
                }
            )

        if json_lines:
            self.print_json_lines(json_lines)

    # expression functions

    def do_addExpressionFunction(self, arg):
        """
        Adds a new expression function

        Syntax:
            addExpressionFunction {json_configuration}

        Examples:
            see listExpressionFunctions for examples of json_configurations

        Caution:
            Adding a new function requires a plugin to be programmed!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FUNCTION"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if self.getRecord("CFG_EFUNC", "EFUNC_CODE", parmData["FUNCTION"]):
            colorize_msg("Function already exists", "warning")
            return

        efuncID = self.getDesiredValueOrNext(
            "CFG_EFUNC", "EFUNC_ID", parmData.get("ID")
        )
        if parmData.get("ID") and efuncID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        parmData["FUNCLIB"] = parmData.get("FUNCLIB", "g2func_lib")
        parmData["VERSION"] = parmData.get("VERSION", 1)
        parmData["CONNECTSTR"] = parmData.get("CONNECTSTR", None)
        parmData["LANGUAGE"] = parmData.get("LANGUAGE", None)
        parmData["JAVACLASSNAME"] = parmData.get("JAVACLASSNAME", None)

        newRecord = {}
        newRecord["EFUNC_ID"] = efuncID
        newRecord["EFUNC_CODE"] = parmData["FUNCTION"]
        newRecord["EFUNC_DESC"] = parmData["FUNCTION"]
        newRecord["FUNC_LIB"] = parmData["FUNCLIB"]
        newRecord["FUNC_VER"] = parmData["VERSION"]
        newRecord["CONNECT_STR"] = parmData["CONNECTSTR"]
        newRecord["LANGUAGE"] = parmData["LANGUAGE"]
        newRecord["JAVA_CLASS_NAME"] = parmData["JAVACLASSNAME"]

        self.cfgData["G2_CONFIG"]["CFG_EFUNC"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Expression function successfully added!", "success")

    def do_listExpressionFunctions(self, arg):
        """
        Returns the list of expression functions

        Syntax:
            listExpressionFunctions [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for funcRecord in sorted(
            self.getRecordList("CFG_EFUNC"), key=lambda k: k["EFUNC_ID"]
        ):
            if arg and arg.lower() not in str(funcRecord).lower():
                continue
            json_lines.append(
                {
                    "id": funcRecord["EFUNC_ID"],
                    "function": funcRecord["EFUNC_CODE"],
                    "version": funcRecord["FUNC_VER"],
                    "connectStr": funcRecord["CONNECT_STR"],
                    "language": funcRecord["LANGUAGE"],
                    "javaClassName": funcRecord["JAVA_CLASS_NAME"],
                }
            )

        if json_lines:
            self.print_json_lines(json_lines)

        return

    # comparison functions

    def do_addComparisonFunction(self, arg):
        """
        Adds a new comparison function

        Syntax:
            addComparisonFunction {json_configuration}

        Examples:
            see listComparisonFunctions for examples of json_configurations

        Caution:
            Adding a new function requires a plugin to be programmed!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FUNCTION"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if self.getRecord("CFG_CFUNC", "CFUNC_CODE", parmData["FUNCTION"]):
            colorize_msg("Function already exists", "warning")
            return

        cfuncID = self.getDesiredValueOrNext(
            "CFG_CFUNC", "CFUNC_ID", parmData.get("ID")
        )
        if parmData.get("ID") and cfuncID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        parmData["FUNCLIB"] = parmData.get("FUNCLIB", "g2func_lib")
        parmData["VERSION"] = parmData.get("VERSION", 1)
        parmData["ANONSUPPORT"] = parmData.get("ANONSUPPORT", "No")
        parmData["CONNECTSTR"] = parmData.get("CONNECTSTR", None)
        parmData["LANGUAGE"] = parmData.get("LANGUAGE", None)
        parmData["JAVACLASSNAME"] = parmData.get("JAVACLASSNAME", None)

        newRecord = {}
        newRecord["CFUNC_ID"] = cfuncID
        newRecord["CFUNC_CODE"] = parmData["FUNCTION"]
        newRecord["CFUNC_DESC"] = parmData["FUNCTION"]
        newRecord["FUNC_LIB"] = parmData["FUNCLIB"]
        newRecord["FUNC_VER"] = parmData["VERSION"]
        newRecord["CONNECT_STR"] = parmData["CONNECTSTR"]
        newRecord["ANON_SUPPORT"] = parmData["ANONSUPPORT"]
        newRecord["LANGUAGE"] = parmData["LANGUAGE"]
        newRecord["JAVA_CLASS_NAME"] = parmData["JAVACLASSNAME"]
        self.cfgData["G2_CONFIG"]["CFG_CFUNC"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Comparison function successfully added!", "success")

    def do_listComparisonFunctions(self, arg):
        """
        Returns the list of comparison functions

        Syntax:
            listComparisonFunctions [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for funcRecord in sorted(
            self.getRecordList("CFG_CFUNC"), key=lambda k: k["CFUNC_ID"]
        ):
            if arg and arg.lower() not in str(funcRecord).lower():
                continue
            json_lines.append(
                {
                    "id": funcRecord["CFUNC_ID"],
                    "function": funcRecord["CFUNC_CODE"],
                    "connectStr": funcRecord["CONNECT_STR"],
                    "anonSupport": funcRecord["ANON_SUPPORT"],
                    "language": funcRecord["LANGUAGE"],
                    "javaClassName": funcRecord["JAVA_CLASS_NAME"],
                }
            )
        if json_lines:
            self.print_json_lines(json_lines)

        return

    # comparison thresholds

    def formatComparisonThresholdJson(self, cfrtnRecord):

        funcRecord = self.getRecord("CFG_CFUNC", "CFUNC_ID", cfrtnRecord["CFUNC_ID"])
        if cfrtnRecord.get("FTYPE_ID", 0) != 0:
            ftypeCode = self.getRecord(
                "CFG_FTYPE", "FTYPE_ID", cfrtnRecord["FTYPE_ID"]
            )["FTYPE_CODE"]
        else:
            ftypeCode = "all"
        return {
            "id": cfrtnRecord["CFRTN_ID"],
            "function": funcRecord["CFUNC_CODE"],
            "returnOrder": cfrtnRecord["EXEC_ORDER"],
            "scoreName": cfrtnRecord["CFUNC_RTNVAL"],
            "feature": ftypeCode,
            "sameScore": cfrtnRecord["SAME_SCORE"],
            "closeScore": cfrtnRecord["CLOSE_SCORE"],
            "likelyScore": cfrtnRecord["LIKELY_SCORE"],
            "plausibleScore": cfrtnRecord["PLAUSIBLE_SCORE"],
            "unlikelyScore": cfrtnRecord["UN_LIKELY_SCORE"],
        }

    def do_addComparisonThreshold(self, arg):
        """
        Adds a new comparison function threshold setting

        Syntax:
            addComparisonThreshold {json_configuration}

        Notes:
            You can override the comparison thresholds for specific features by specifying the feature instead of all.
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FUNCTION", "SCORENAME"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FEATURE"] = parmData.get("FEATURE", "ALL")
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
            parmData["SCORENAME"] = parmData["SCORENAME"].upper()
            parmData["SAMESCORE"] = int(parmData.get("SAMESCORE", 100))
            parmData["CLOSESCORE"] = int(parmData.get("CLOSESCORE", 90))
            parmData["LIKELYSCORE"] = int(parmData.get("LIKELYSCORE", 80))
            parmData["PLAUSIBLESCORE"] = int(parmData.get("PLAUSIBLESCORE", 70))
            parmData["UNLIKELYSCORE"] = int(parmData.get("UNLIKELYSCORE", 60))
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        cfrtnID = self.getDesiredValueOrNext(
            "CFG_CFRTN", "CFRTN_ID", parmData.get("ID")
        )
        if parmData.get("ID") and cfrtnID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        cfuncRecord, message = self.lookupComparisonFunction(parmData["FUNCTION"])
        if not cfuncRecord:
            colorize_msg(message, "warning")
            return
        cfuncID = cfuncRecord["CFUNC_ID"]

        ftypeID = 0
        if "FEATURE" in parmData and parmData["FEATURE"].upper() != "ALL":
            ftypeRecord, message = self.lookupFeature(parmData["FEATURE"])
            if not ftypeRecord:
                colorize_msg(message, "error")
                return
            ftypeID = ftypeRecord["FTYPE_ID"]

        cfcallRecord = self.getRecord(
            "CFG_CFRTN",
            ["CFUNC_ID", "CFUNC_RTNVAL", "FTYPE_ID"],
            [cfuncID, parmData["SCORENAME"], ftypeID],
        )
        if cfcallRecord:
            colorize_msg(
                f"Comparison threshold function: {parmData['FUNCTION']}, return code: {parmData['SCORENAME']} for feature {parmData['FEATURE']} already set",
                "warning",
            )
            return

        # see if the return value already has an exec order and use it! must be in the expected order
        cfcallRecord = self.getRecord(
            "CFG_CFRTN",
            ["CFUNC_ID", "CFUNC_RTNVAL", "FTYPE_ID"],
            [cfuncID, parmData["SCORENAME"], 0],
        )
        if cfcallRecord:
            execOrder = cfcallRecord["EXEC_ORDER"]
        elif parmData.get("EXECORDER"):
            execOrder = parmData.get("EXECORDER")
        else:
            execOrder = self.getDesiredValueOrNext(
                "CFG_CFRTN", ["CFUNC_ID", "FTYPE_ID", "EXEC_ORDER"], [cfuncID, 0, 0]
            )

        newRecord = {}
        newRecord["CFRTN_ID"] = cfrtnID
        newRecord["CFUNC_ID"] = cfuncID
        newRecord["FTYPE_ID"] = ftypeID
        newRecord["CFUNC_RTNVAL"] = parmData["SCORENAME"]
        newRecord["EXEC_ORDER"] = execOrder
        newRecord["SAME_SCORE"] = parmData["SAMESCORE"]
        newRecord["CLOSE_SCORE"] = parmData["CLOSESCORE"]
        newRecord["LIKELY_SCORE"] = parmData["LIKELYSCORE"]
        newRecord["PLAUSIBLE_SCORE"] = parmData["PLAUSIBLESCORE"]
        newRecord["UN_LIKELY_SCORE"] = parmData["UNLIKELYSCORE"]
        self.cfgData["G2_CONFIG"]["CFG_CFRTN"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Comparison threshold successfully added!", "success")

    def do_setComparisonThreshold(self, arg):
        """
        Sets the comparison thresholds for a particular comparison threshold ID

        Syntax:
            setComparisonThreshold {partial_json_configuration}

        Example:
            setComparisonThreshold {"id": 9, "sameScore": 100, "closeScore": 92, "likelyScore": 90, "plausibleScore": 85, "unlikelyScore": 75}

        Notes:
            Only the scores can be changed here.
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ID"])
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        oldRecord = self.getRecord("CFG_CFRTN", "CFRTN_ID", parmData["ID"])
        if not oldRecord:
            colorize_msg("Comparison threshold ID not found", "error")
            return

        oldParmData = dictKeysUpper(self.formatComparisonThresholdJson(oldRecord))
        settable_parm_list = (
            "RETURNORDER",
            "SAMESCORE",
            "CLOSESCORE",
            "LIKELYSCORE",
            "PLAUSIBLESCORE",
            "UNLIKELYSCORE",
        )
        newParmData = self.settable_parms(oldParmData, parmData, settable_parm_list)
        if newParmData.get("errors"):
            colorize_msg(newParmData["errors"], "error")
            return
        if newParmData["update_cnt"] == 0:
            colorize_msg("No changes detected", "warning")
            return

        newRecord = dict(oldRecord)  # must use dict to create a new instance
        newRecord["EXEC_ORDER"] = parmData["RETURNORDER"]
        newRecord["SAME_SCORE"] = parmData["SAMESCORE"]
        newRecord["CLOSE_SCORE"] = parmData["CLOSESCORE"]
        newRecord["LIKELY_SCORE"] = parmData["LIKELYSCORE"]
        newRecord["PLAUSIBLE_SCORE"] = parmData["PLAUSIBLESCORE"]
        newRecord["UN_LIKELY_SCORE"] = parmData["UNLIKELYSCORE"]

        self.cfgData["G2_CONFIG"]["CFG_CFRTN"].remove(oldRecord)
        self.cfgData["G2_CONFIG"]["CFG_CFRTN"].append(newRecord)
        colorize_msg("Comparison threshold successfully updated!", "success")
        self.configUpdated = True

    def do_listComparisonThresholds(self, arg):
        """
        Returns the list of thresholds by comparison function return value

        Syntax:
            listComparisonThresholds [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for cfrtnRecord in sorted(
            self.getRecordList("CFG_CFRTN"),
            key=lambda k: (k["CFUNC_ID"], k["CFRTN_ID"]),
        ):
            cfrtnJson = self.formatComparisonThresholdJson(cfrtnRecord)
            if arg and arg.lower() not in str(cfrtnJson).lower():
                continue
            json_lines.append(cfrtnJson)
        if json_lines:
            self.print_json_lines(json_lines)
        print()

    def do_deleteComparisonThreshold(self, arg):
        """
        Deletes a comparison threshold

        Syntax:
           deleteComparisonThreshold id
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = (
                dictKeysUpper(json.loads(arg))
                if arg.startswith("{")
                else {"ID": int(arg)}
            )
            parmData["ID"] = (
                int(parmData["ID"])
                if isinstance(parmData["ID"], str) and parmData["ID"].isdigit()
                else parmData["ID"]
            )
            self.validate_parms(parmData, ["ID"])
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        cfrtnRecord = self.getRecord("CFG_CFRTN", "CFRTN_ID", parmData["ID"])
        if not cfrtnRecord:
            colorize_msg(
                f"Comparison threshold ID {parmData['ID']} does not exist", "warning"
            )
            return

        self.cfgData["G2_CONFIG"]["CFG_CFRTN"].remove(cfrtnRecord)
        colorize_msg("Comparison threshold successfully deleted!", "success")
        self.configUpdated = True

    # distinct functions

    def do_listDistinctFunctions(self, arg):
        """
        Returns the list of distinct functions

        Syntax:
            listDistinctFunctions [filter_expression] [table|json|jsonl]
        """
        arg = self.check_arg_for_output_format("list", arg)
        json_lines = []
        for funcRecord in sorted(
            self.getRecordList("CFG_DFUNC"), key=lambda k: k["DFUNC_ID"]
        ):
            if arg and arg.lower() not in str(funcRecord).lower():
                continue
            json_lines.append(
                {
                    "id": funcRecord["DFUNC_ID"],
                    "function": funcRecord["DFUNC_CODE"],
                    "connectStr": funcRecord["CONNECT_STR"],
                    "anonSupport": funcRecord["ANON_SUPPORT"],
                    "language": funcRecord["LANGUAGE"],
                    "javaClassName": funcRecord["JAVA_CLASS_NAME"],
                }
            )

        if json_lines:
            self.print_json_lines(json_lines)

    def do_addDistinctFunction(self, arg):
        """
        Adds a new distinct function

        Syntax:
            addDistinctFunction {json_configuration}

        Examples:
            see listDistinctFunctions for examples of json_configurations

        Caution:
            Adding a new function requires a plugin to be programmed!
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["FUNCTION"])
            parmData["ID"] = parmData.get("ID", 0)
            parmData["FUNCTION"] = parmData["FUNCTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if self.getRecord("CFG_DFUNC", "DFUNC_CODE", parmData["FUNCTION"]):
            colorize_msg("Function already exists", "warning")
            return

        dfuncID = self.getDesiredValueOrNext(
            "CFG_DFUNC", "DFUNC_ID", parmData.get("ID")
        )
        if parmData.get("ID") and dfuncID != parmData["ID"]:
            colorize_msg(
                "The specified ID is already taken (remove it to assign the next available)",
                "error",
            )
            return

        parmData["FUNCLIB"] = parmData.get("FUNCLIB", "g2func_lib")
        parmData["VERSION"] = parmData.get("VERSION", 1)
        parmData["CONNECTSTR"] = parmData.get("CONNECTSTR", None)
        parmData["ANONSUPPORT"] = parmData.get("ANONSUPPORT", None)
        parmData["LANGUAGE"] = parmData.get("LANGUAGE", None)
        parmData["JAVACLASSNAME"] = parmData.get("JAVACLASSNAME", None)

        newRecord = {}
        newRecord["DFUNC_ID"] = dfuncID
        newRecord["DFUNC_CODE"] = parmData["FUNCTION"]
        newRecord["DFUNC_DESC"] = parmData["FUNCTION"]
        newRecord["FUNC_LIB"] = parmData["FUNCLIB"]
        newRecord["FUNC_VER"] = parmData["VERSION"]
        newRecord["ANON_SUPPORT"] = parmData["ANONSUPPORT"]
        newRecord["CONNECT_STR"] = parmData["CONNECTSTR"]
        newRecord["LANGUAGE"] = parmData["LANGUAGE"]
        newRecord["JAVA_CLASS_NAME"] = parmData["JAVACLASSNAME"]
        self.cfgData["G2_CONFIG"]["CFG_DFUNC"].append(newRecord)
        self.configUpdated = True
        colorize_msg("Distinct function successfully added!", "success")

    # ===== other miscellaneous functions =====

    # Compatibility version commands

    def do_verifyCompatibilityVersion(self, arg):
        """
        Verify if the current configuration is compatible with a specific version number

        Examples:
            verifyCompatibilityVersion {"expectedVersion": "2"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        this_version = self.cfgData["G2_CONFIG"]["CONFIG_BASE_VERSION"][
            "COMPATIBILITY_VERSION"
        ]["CONFIG_VERSION"]
        if this_version != parmData["EXPECTEDVERSION"]:
            colorize_msg(f"Incompatible! This is version {this_version}", "error")
            if self.isInteractive is False:
                raise Exception("Incorrect compatibility version.")
        else:
            colorize_msg("Compatibility version successfully verified", "success")

    def do_updateCompatibilityVersion(self, arg):
        """
        Update the compatibility version of this configuration

        Examples:
            updateCompatibilityVersion {"fromVersion": "1", "toVersion": "2"}
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        this_version = self.cfgData["G2_CONFIG"]["CONFIG_BASE_VERSION"][
            "COMPATIBILITY_VERSION"
        ]["CONFIG_VERSION"]
        if this_version != parmData["FROMVERSION"]:
            colorize_msg(
                f"From version mismatch. This is version {this_version}", "error"
            )
            return

        self.cfgData["G2_CONFIG"]["CONFIG_BASE_VERSION"]["COMPATIBILITY_VERSION"][
            "CONFIG_VERSION"
        ] = parmData["TOVERSION"]
        self.configUpdated = True
        colorize_msg("Compatibility version successfully updated!", "success")

    def do_getCompatibilityVersion(self, arg):
        """
        Retrieve the compatibility version of this configuration

        Syntax:
            getCompatibilityVersion
        """
        try:
            this_version = self.cfgData["G2_CONFIG"]["CONFIG_BASE_VERSION"][
                "COMPATIBILITY_VERSION"
            ]["CONFIG_VERSION"]
            colorize_msg(f"Compatibility version is {this_version}", "success")
        except KeyError:
            colorize_msg("Could not retrieve compatibility version", "error")

    # ===== config sections =====

    def do_getConfigSection(self, arg):
        """
        Returns the json configuration for a specific configuration table

        Syntax:
            getConfigSection [section name] [filter_expression] [table|json|jsonl]

        Examples:
            getConfigSection CFG_CFUNC

        Caution:
            This command should only be used by Senzing engineers
        """
        arg = self.check_arg_for_output_format(
            "list", arg
        )  # checking for list here even though a get
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return

        section_name = arg.split()[0]
        filter_str = None
        if len(arg.split()) > 1:
            filter_str = arg.replace(section_name, "").strip()
            print(f"\nfilter: {filter_str}\n")

        if self.cfgData["G2_CONFIG"].get(section_name):
            if not filter_str:
                self.print_json_lines(self.cfgData["G2_CONFIG"][section_name])
            else:
                output_rows = []
                for record in self.cfgData["G2_CONFIG"][section_name]:
                    if filter_str.lower() in json.dumps(record).lower():
                        output_rows.append(record)
                self.print_json_lines(output_rows)

        elif section_name in self.cfgData["G2_CONFIG"]:
            colorize_msg("Configuration section is empty", "warning")
        else:
            colorize_msg("Configuration section not found", "error")

    def do_addConfigSection(self, arg):
        """
        Adds a new configuration section

        Syntax:
            addConfigSection {json_configuration}

        Caution:
            This command should only be used by Senzing engineers
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = (
                dictKeysUpper(json.loads(arg))
                if arg.startswith("{")
                else {"SECTION": arg}
            )
            self.validate_parms(parmData, ["SECTION"])
            parmData["SECTION"] = parmData["SECTION"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if parmData["SECTION"] in self.cfgData["G2_CONFIG"]:
            colorize_msg("Configuration section already exists!", "error")
            return

        self.cfgData["G2_CONFIG"][parmData["SECTION"]] = []
        self.configUpdated = True
        colorize_msg("Configuration section successfully added!", "success")

    def do_addConfigSectionField(self, arg):
        """
        Adds a new field to an existing configuration section

        Syntax:
            addConfigSectionField {json_configuration}

        Caution:
            This command should only be used by Senzing engineers
        """
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["SECTION", "FIELD", "VALUE"])
            parmData["SECTION"] = parmData["SECTION"].upper()
            parmData["FIELD"] = parmData["FIELD"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        if parmData["SECTION"] not in self.cfgData["G2_CONFIG"]:
            colorize_msg("Configuration section does not exist", "error")
            return

        # update every record that needs it
        existed_cnt = updated_cnt = 0
        for i in range(len(self.cfgData["G2_CONFIG"][parmData["SECTION"]])):
            if parmData["FIELD"] in self.cfgData["G2_CONFIG"][parmData["SECTION"]][i]:
                existed_cnt += 1
            else:
                self.cfgData["G2_CONFIG"][parmData["SECTION"]][i][parmData["FIELD"]] = (
                    parmData["VALUE"]
                )
                updated_cnt += 1

        if existed_cnt > 0:
            colorize_msg(f"Field already existed on {existed_cnt} records", "warning")
        if updated_cnt > 0:
            self.configUpdated = True
            colorize_msg(
                f"Configuration section field successfully added to {updated_cnt} records!",
                "success",
            )

    # ===== system parameters  =====

    def do_listSystemParameters(self, arg):
        """\nlistSystemParameters\n"""

        for i in self.cfgData["G2_CONFIG"]["CFG_RTYPE"]:
            if i["RCLASS_ID"] == 2:
                print(f'\n{{"relationshipsBreakMatches": "{i["BREAK_RES"]}"}}\n')
                break

    def do_setSystemParameter(self, arg):
        """\nsetSystemParameter {"parameter": "<value>"}\n"""

        validParameters = "relationshipsBreakMatches"
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = json.loads(arg)  # don't want these upper
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        # not really expecting a list here, getting the dictionary key they used
        for parameterCode in parmData:
            parameterValue = parmData[parameterCode]

            if parameterCode not in validParameters:
                colorize_msg("%s is an invalid system parameter" % parameterCode, "B")

            # set all disclosed relationship types to break or not break matches
            elif parameterCode == "relationshipsBreakMatches":
                if parameterValue.upper() in ("YES", "Y"):
                    breakRes = 1
                elif parameterValue.upper() in ("NO", "N"):
                    breakRes = 0
                else:
                    colorize_msg(
                        "%s is an invalid parameter for %s"
                        % (parameterValue, parameterCode),
                        "B",
                    )
                    return

                for i in range(len(self.cfgData["G2_CONFIG"]["CFG_RTYPE"])):
                    if self.cfgData["G2_CONFIG"]["CFG_RTYPE"][i]["RCLASS_ID"] == 2:
                        self.cfgData["G2_CONFIG"]["CFG_RTYPE"][i][
                            "BREAK_RES"
                        ] = breakRes
                        self.configUpdated = True

    def do_touch(self, arg):
        """\nMarks configuration object as modified when no configuration changes have been applied yet.\n"""

        # This is a no-op. It marks the configuration as modified, without doing anything to it.
        self.configUpdated = True
        print()

    # ===== Deprecated/replaced commands =====

    def print_replacement(self, old, new):
        print(
            colorize(
                f"\n{old[3:]} has been replaced, please use {new[3:]} in the future.\n",
                "dim,italics",
            )
        )

    def do_addStandardizeFunc(self, arg):
        self.do_addStandardizeFunction(arg)
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_addStandardizeFunction"
        )

    def do_addExpressionFunc(self, arg):
        self.do_addExpressionFunction(arg)
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_addExpressionFunction"
        )

    def do_addComparisonFunc(self, arg):
        self.do_addComparisonFunction(arg)
        self.print_replacement(sys._getframe(0).f_code.co_name, "do_addComparisonFunc")

    def do_addComparisonFuncReturnCode(self, arg):
        self.do_addComparisonThreshold(arg)
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_addComparisonThreshold"
        )

    def do_addFeatureComparison(self, arg):
        self.do_addComparisonCall(arg)
        self.print_replacement(sys._getframe(0).f_code.co_name, "do_addComparisonCall")

    def do_deleteFeatureComparison(self, arg):
        self.do_deleteComparisonCall(arg)
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_deleteComparisonCall"
        )

    def do_addFeatureComparisonElement(self, arg):
        self.do_addComparisonCallElement(
            addAttributeToArg(arg, add={"callType": "comparison"})
        )
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_addComparisonCallElement"
        )

    def do_deleteFeatureComparisonElement(self, arg):
        self.do_deleteComparisonCallElement(
            addAttributeToArg(arg, add={"callType": "comparison"})
        )
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_deleteComparisonCallElement"
        )

    def do_addFeatureDistinctCallElement(self, arg):
        self.do_addDistinctCallElement(
            addAttributeToArg(arg, add={"callType": "distinct"})
        )
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_addDistinctCallElement"
        )

    def do_setFeatureElementDerived(self, arg):
        self.do_setFeatureElement(arg)
        self.print_replacement(sys._getframe(0).f_code.co_name, "do_setFeatureElement")

    def do_setFeatureElementDisplayLevel(self, arg):
        self.do_setFeatureElement(
            addAttributeToArg(arg, rename="display=display_level")
        )
        self.print_replacement(sys._getframe(0).f_code.co_name, "do_setFeatureElement")

    def do_addEntityScore(self, arg):
        print(
            colorize(
                "\nThis configuration command is no longer needed\n", "dim,italics"
            )
        )

    def do_addToNameSSNLast4hash(self, arg):
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ELEMENT"])
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        parmData["CALLTYPE"] = "expression"
        call_id, message = self.getCallID(
            "SSN_LAST4", parmData["CALLTYPE"], "EXPRESS_BOM"
        )
        if not call_id:
            colorize_msg(message, "error")
            return

        parmData["ID"] = call_id
        self.addCallElement(json.dumps(parmData))
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_addExpressionCallElement"
        )

    def do_deleteFromSSNLast4hash(self, arg):
        if not arg:
            self.do_help(sys._getframe(0).f_code.co_name)
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
            self.validate_parms(parmData, ["ELEMENT"])
            parmData["ELEMENT"] = parmData["ELEMENT"].upper()
        except Exception as err:
            colorize_msg(f"Command error: {err}", "error")
            return

        parmData["CALLTYPE"] = "expression"
        call_id, message = self.getCallID(
            "SSN_LAST4", parmData["CALLTYPE"], "EXPRESS_BOM"
        )
        if not call_id:
            colorize_msg(message, "error")
            return

        parmData["ID"] = call_id
        self.deleteCallElement(json.dumps(parmData))
        self.print_replacement(
            sys._getframe(0).f_code.co_name, "do_deleteExpressionCallElement"
        )

    def do_updateAttributeAdvanced(self, arg):
        self.do_setAttribute(arg)
        self.print_replacement(sys._getframe(0).f_code.co_name, "do_setAttribute")

    def do_updateFeatureVersion(self, arg):
        self.do_setFeature(arg)
        self.print_replacement(sys._getframe(0).f_code.co_name, "do_Feature")

    # ===== Class Utils =====

    def print_json_record(self, json_obj):
        if type(json_obj) not in [dict, list]:
            json_obj = json.loads(json_obj)

        if self.current_output_format_record == "table":
            render_string = self.print_json_as_table(
                json_obj if type(json_obj) == list else [json_obj]
            )
            self.print_scrolling(render_string)
            return

        if self.current_output_format_record == "json":
            json_str = json.dumps(json_obj, indent=4)
        else:
            json_str = json.dumps(json_obj)

        if self.pygmentsInstalled:
            render_string = highlight(
                json_str, lexers.JsonLexer(), formatters.TerminalFormatter()
            )
        else:
            render_string = colorize_json(json_str)

        print(f"\n{render_string}\n")

    def print_json_lines(self, json_lines, display_header=""):
        if not json_lines:
            colorize_msg("Nothing to display", "warning")
            return

        if display_header:
            print(f"\n{display_header}")

        if self.current_output_format_list == "table":
            render_string = self.print_json_as_table(json_lines)
        elif self.current_output_format_list == "jsonl":
            render_string = ""
            for line in json_lines:
                if self.pygmentsInstalled:
                    render_string += (
                        highlight(
                            json.dumps(line),
                            lexers.JsonLexer(),
                            formatters.TerminalFormatter(),
                        ).replace("\n", "")
                        + "\n"
                    )
                else:
                    render_string += colorize_json(json.dumps(line)) + "\n"
        else:
            json_doc = "["
            for line in json_lines:
                json_doc += json.dumps(line) + ", "
            json_doc = json_doc[0:-2] + "]"
            render_string = colorize_json(json.dumps(json.loads(json_doc), indent=4))

        self.print_scrolling(render_string)

    def print_json_as_table(self, json_lines):
        tblColumns = list(json_lines[0].keys())
        columnHeaderList = []
        for attr_name in tblColumns:
            columnHeaderList.append(colorize(attr_name, "attr_color"))
        table_object = prettytable.PrettyTable()
        table_object.field_names = columnHeaderList
        row_count = 0
        for json_data in json_lines:
            row_count += 1
            tblRow = []
            for attr_name in tblColumns:
                attr_value = (
                    json.dumps(json_data[attr_name])
                    if type(json_data[attr_name]) in (list, dict)
                    else str(json_data[attr_name])
                )
                if row_count % 2 == 0:  # for future alternating colors
                    tblRow.append(colorize(attr_value, "dim"))
                else:
                    tblRow.append(colorize(attr_value, "dim"))
            table_object.add_row(tblRow)

        table_object.align = "l"
        if hasattr(prettytable, "SINGLE_BORDER"):
            table_object.set_style(prettytable.SINGLE_BORDER)
        table_object.hrules = 1
        render_string = table_object.get_string()
        return render_string

    def print_scrolling(self, render_string):
        less = subprocess.Popen(["less", "-FMXSR"], stdin=subprocess.PIPE)
        try:
            less.stdin.write(render_string.encode("utf-8"))
        except IOError:
            pass
        less.stdin.close()
        less.wait()
        print()


# ===== Utility functions =====


def getFeatureBehavior(feature):
    featureBehavior = feature["FTYPE_FREQ"]
    if str(feature["FTYPE_EXCL"]).upper() in ("1", "Y", "YES"):
        featureBehavior += "E"
    if str(feature["FTYPE_STAB"]).upper() in ("1", "Y", "YES"):
        featureBehavior += "S"
    return featureBehavior


def parseFeatureBehavior(behaviorCode):
    behaviorDict = {"EXCLUSIVITY": "No", "STABILITY": "No"}
    if behaviorCode not in ("NAME", "NONE"):
        if "E" in behaviorCode:
            behaviorDict["EXCLUSIVITY"] = "Yes"
            behaviorCode = behaviorCode.replace("E", "")
        if "S" in behaviorCode:
            behaviorDict["STABILITY"] = "Yes"
            behaviorCode = behaviorCode.replace("S", "")
    if behaviorCode in ("A1", "F1", "FF", "FM", "FVM", "NONE", "NAME"):
        behaviorDict["FREQUENCY"] = behaviorCode
    else:
        behaviorDict = None
    return behaviorDict


def dictKeysUpper(dictionary):
    if isinstance(dictionary, list):
        return [v.upper() for v in dictionary]
    elif isinstance(dictionary, dict):
        return {k.upper(): v for k, v in dictionary.items()}
    else:
        return dictionary


def addAttributeToArg(arg, **kwargs):
    # add={"callType": "expression"}
    # rename=display=display_level
    if arg:
        parmData = json.loads(arg)
        if kwargs.get("add"):
            parmData.update(kwargs.get("add"))
        if kwargs.get("rename"):
            new, old = kwargs.get("rename").split("=")
            parmData[new] = parmData.get(old)
        arg = json.dumps(parmData)
    return arg


if __name__ == "__main__":

    argParser = argparse.ArgumentParser()
    argParser.add_argument("fileToProcess", default=None, nargs="?")
    argParser.add_argument(
        "-c",
        "--ini-file-name",
        dest="ini_file_name",
        default=None,
        help="name of a G2Module.ini file to use",
    )
    argParser.add_argument(
        "-f",
        "--force",
        dest="forceMode",
        default=False,
        action="store_true",
        help="when reading from a file, execute each command without prompts",
    )
    argParser.add_argument(
        "-H",
        "--histDisable",
        dest="histDisable",
        action="store_true",
        default=False,
        help="disable history file usage",
    )
    args = argParser.parse_args()

    # Check if INI file or env var is specified, otherwise use default INI file
    iniFileName = None

    if args.ini_file_name:
        iniFileName = pathlib.Path(args.ini_file_name)
    elif os.getenv("SENZING_ENGINE_CONFIGURATION_JSON"):
        g2module_params = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON")
    else:
        iniFileName = pathlib.Path(G2Paths.get_G2Module_ini_path())

    if iniFileName:
        G2Paths.check_file_exists_and_readable(iniFileName)
        iniParamCreator = G2IniParams()
        g2module_params = iniParamCreator.getJsonINIParams(iniFileName)

    cmd_obj = G2CmdShell(
        g2module_params, args.histDisable, args.forceMode, args.fileToProcess
    )

    if args.fileToProcess:
        cmd_obj.fileloop()
    else:
        cmd_obj.cmdloop()

    sys.exit()
