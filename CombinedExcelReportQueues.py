#!/usr/bin/python

"""
This class consolidates all the thirteen excelsheets into one workbook.
"""

__author__ = "Anshita Saxena"
__copyright__ = "(c) Copyright IBM 2019"
__credits__ = ["BAT DMS IBM Team"]
__maintainer__ = "Anshita Saxena"
__email__ = "anshita333saxena@gmail.com"
__status__ = "Production"

# Libraries for writing, reading the excelsheet and finding pathnames.
import xlsxwriter
import xlrd
import glob


class CombinedExcelReport:
    """
    To use your method without an instance of a class you can attach a class
    method decorator
    """

    @classmethod
    def message_types_together(
            self, G_LAG_REPORT_MAIN_OUTPUT_FILE,
            G_LAG_REPORT_COMBINED_MAIN_OUTPUT_FILE,
            G_LAG_REPORT_OUTPUT_FILE=None,
            G_LAG_REPORT_COMBINED_OUTPUT_FILE=None):

        # For Transmit Layer, retrieve path and record values
        if G_LAG_REPORT_OUTPUT_FILE is not None \
                and G_LAG_REPORT_COMBINED_OUTPUT_FILE is not None:
            workbookw = xlsxwriter.Workbook(G_LAG_REPORT_COMBINED_OUTPUT_FILE)
            """
            Retrieving the correct path to combine message type sheets into
            one workbook
            """
            gpath = G_LAG_REPORT_OUTPUT_FILE
            partial_path = [x for x in gpath.split('/')[0:-1]]
            partial_path = str(str(str(str(str(str(str(
                partial_path).replace(
                "'", "")).replace(
                "[", "")).replace(
                "]", "")).replace(
                ",", "/")).replace(
                " ", "")).replace(
                "u/u", "/")).replace(
                "/u", "/")
            for filename in glob.glob(partial_path + "/*.xlsx"):
                workbook = xlrd.open_workbook(filename)
                before_message_type, rest_filename = filename.split(
                    "_Lag_", 1)
                message_type = str(before_message_type.split('/')[-1])
                message_type = message_type.replace(
                    "-3-7", "\
                                    ") if message_type.__contains__("-3-7\
                                    ") else message_type
                worksheet = workbook.sheet_by_name(message_type)
                worksheetw = workbookw.add_worksheet(message_type)
                """
                Read the values from one worksheet and copy them into
                combined workbook
                """
                for i in range(0, worksheet.nrows):
                    for j in range(0, worksheet.ncols):
                        value = worksheet.cell_value(i, j)
                        worksheetw.write(i, j, value)

                worksheetw.set_column('A:A', 13.3)
                worksheetw.freeze_panes(0, 1)

            workbookw.close()

        # For all the layers except Transmit
        workbookw = xlsxwriter.Workbook(G_LAG_REPORT_COMBINED_MAIN_OUTPUT_FILE)
        gpath = G_LAG_REPORT_MAIN_OUTPUT_FILE
        partial_path = [x for x in gpath.split('/')[0:-1]]
        partial_path = str(str(str(str(str(str(str(
            partial_path).replace(
            "'", "")).replace(
            "[", "")).replace(
            "]", "")).replace(
            ",", "/")).replace(
            " ", "")).replace(
            "u/u", "/")).replace(
            "/u", "/")
        for filename in glob.glob(partial_path + "/*.xlsx"):
            workbook = xlrd.open_workbook(filename)
            before_message_type, rest_filename = filename.split("_Lag_", 1)
            message_type = str(before_message_type.split('/')[-1])
            message_type = message_type.replace(
                "-3-7", "\
                                ") if message_type.__contains__("-3-7\
                                ") else message_type
            worksheetf = workbook.sheet_by_name('FRA_' + message_type)
            worksheeta = workbook.sheet_by_name('AMS_' + message_type)
            worksheetwf = workbookw.add_worksheet('FRA_' + message_type)
            worksheetwa = workbookw.add_worksheet('AMS_' + message_type)
            for i in range(0, worksheetf.nrows):
                for j in range(0, worksheetf.ncols):
                    value = worksheetf.cell_value(i, j)
                    worksheetwf.write(i, j, value)
            for i in range(0, worksheeta.nrows):
                for j in range(0, worksheeta.ncols):
                    value = worksheeta.cell_value(i, j)
                    worksheetwa.write(i, j, value)

            # Set width of some columns for Frankfurt and Amsterdam Reports
            worksheetwf.set_column('A:A', 40.00)
            worksheetwf.set_column('B:B', 40.00)
            worksheetwf.set_column('C:C', 3.00)
            worksheetwa.set_column('A:A', 40.00)
            worksheetwa.set_column('B:B', 40.00)
            worksheetwa.set_column('C:C', 3.00)
            # Freezing columns
            worksheetwf.freeze_panes(0, 1)
            worksheetwf.freeze_panes(0, 2)
            worksheetwf.freeze_panes(0, 3)
            worksheetwa.freeze_panes(0, 1)
            worksheetwa.freeze_panes(0, 2)
            worksheetwa.freeze_panes(0, 3)

        workbookw.close()
