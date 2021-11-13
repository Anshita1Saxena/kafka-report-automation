#!/usr/bin/python

"""
This code updates lag and logSize into excelsheets.
"""

__author__ = "Anshita Saxena"
__copyright__ = "(c) Copyright IBM 2019"
__credits__ = ["BAT DMS IBM Team"]
__maintainer__ = "Anshita Saxena"
__email__ = "anshita333saxena@gmail.com"
__status__ = "Production"

# Libraries for writing, reading the excelsheet and for retrieving date.
import xlsxwriter
import xlrd
import datetime


class ExcelReportGenerate:
    """
    To use your method without an instance of a class you can attach a class
    method decorator
    """
    @classmethod
    def excelSheetUpdation(
            self, currentTotalLagFRA, currentTotalLagAMS,
            currentTotalLagFRAList, currentTotalLagAMSList,
            date_uktime_format, time_uktime_format,
            currentTotalLogSizeFRA, currentTotalLogSizeAMS,
            currentTotalLogSizeFRAList, currentTotalLogSizeAMSList,
            message_type, G_LAG_REPORT_MAIN_OUTPUT_FILE,
            G_LAG_REPORT_OUTPUT_FILE=None):

        num_columns = 0
        global workbook
        basestring = (str, bytes)

        # Retrieve the path for Lag Report having only Lag
        if G_LAG_REPORT_OUTPUT_FILE is not None:
            gpath = G_LAG_REPORT_OUTPUT_FILE
            extract_name = gpath.split('/')[-1]
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
            full_path = partial_path + '/' + message_type + '_' + extract_name
            message_type = message_type.replace("-3-7", "") \
                if message_type.__contains__("-3-7") \
                else message_type

            # Open the specific message type worksheet
            workbook = xlrd.open_workbook(full_path)
            worksheet = workbook.sheet_by_name(message_type)
            # Add the worksheet into workbook
            workbookw = xlsxwriter.Workbook(full_path)
            worksheetw = workbookw.add_worksheet(message_type)

            # Cell Formating of the worksheet
            cell_format = workbookw.add_format({'bold': True})
            cell_format_time = workbookw.add_format({
                'bold': True,
                'font_color': 'red'})

            num_rows = worksheet.nrows - 1

            # Record values in the worksheet
            for i in range(0, num_rows + 1):
                num_columns = worksheet.ncols - 1
                for j in range(0, num_columns + 1):
                    value = worksheet.cell_value(i, j)
                    if i == 0 and j != 0:
                        if isinstance(value, basestring):
                            worksheetw.write(i, j, value, cell_format)
                        elif isinstance(value, float):
                            value_as_datetime = datetime.datetime(
                                *xlrd.xldate_as_tuple(
                                    value, workbook.datemode))
                            worksheetw.write(
                                i, j,
                                value_as_datetime.date().strftime("%d-%b"),
                                cell_format)
                    else:
                        if j == 0:
                            worksheetw.write(i, j, value, cell_format)
                        elif i == 1 and j != 0:
                            worksheetw.write(i, j, value, cell_format_time)
                        else:
                            worksheetw.write(i, j, value)

            date_uktime_format = date_uktime_format.replace(" ", "-")
            date_uktime_format = datetime.datetime.strptime(
                date_uktime_format, '%d-%B')
            date_uktime_format = date_uktime_format.strftime("%d-%B")
            worksheetw.set_column('A:A', 13.3)
            worksheetw.write(
                0, num_columns + 1, date_uktime_format, cell_format)
            worksheetw.write(
                1, num_columns + 1, time_uktime_format, cell_format_time)
            worksheetw.write(num_rows - 1, num_columns + 1, currentTotalLagAMS)
            worksheetw.write(num_rows, num_columns + 1, currentTotalLagFRA)
            # Freeze first column while scrolling to the right
            worksheetw.freeze_panes(0, 1)
            workbookw.close()

        # Retrieve the path for Lag Report having Lag and LogSize
        gpath = G_LAG_REPORT_MAIN_OUTPUT_FILE
        extract_name = gpath.split('/')[-1]
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
        if G_LAG_REPORT_OUTPUT_FILE is not None:
            full_path = partial_path + '/' + message_type + '-3-7_' \
                        + extract_name if message_type.__contains__("\
                        reportDeliveryVanToRetail\
                        ") else partial_path + '/' + message_type \
                        + '_' + extract_name
        else:
            full_path = partial_path + '/' + message_type + '_' + extract_name

        message_type = message_type.replace("-3-7", "") \
            if message_type.__contains__("-3-7") else message_type
        workbooke = xlrd.open_workbook(full_path)
        worksheetef = workbooke.sheet_by_name('FRA_' + message_type)
        worksheetea = workbooke.sheet_by_name('AMS_' + message_type)

        workbookr = xlsxwriter.Workbook(full_path)
        worksheetrf = workbookr.add_worksheet('FRA_' + message_type)
        worksheetra = workbookr.add_worksheet('AMS_' + message_type)

        # Record the values for both Amsterdam and Frankfurt.
        num_rows = worksheetef.nrows - 1
        try:
            for i in range(0, num_rows + 1):
                num_columns = worksheetef.ncols - 1
                for j in range(0, num_columns + 1):
                    try:
                        valuef = worksheetef.cell_value(i, j)
                        valuea = worksheetea.cell_value(i, j)
                        if i == 0 and j != 0 and isinstance(
                                valuef, basestring) or isinstance(
                                                            valuea,
                                                            basestring):
                            worksheetrf.write(i, j, valuef)
                            worksheetra.write(i, j, valuea)
                        elif isinstance(valuef, float) or isinstance(
                                valuea, float):
                            value_as_datetimef = datetime.datetime(
                                *xlrd.xldate_as_tuple(
                                    valuef, workbook.datemode))
                            value_as_datetimea = datetime.datetime(
                                *xlrd.xldate_as_tuple(
                                    valuea, workbook.datemode))
                            worksheetrf.write(
                                i, j,
                                value_as_datetimef.date().strftime("%d-%b"))
                            worksheetra.write(
                                i, j,
                                value_as_datetimea.date().strftime("%d-%b"))
                        else:
                            if j == 0:
                                worksheetrf.write(i, j, valuef)
                                worksheetra.write(i, j, valuea)
                            elif i == 1 and j != 0:
                                worksheetrf.write(i, j, valuef)
                                worksheetra.write(i, j, valuea)
                            else:
                                worksheetrf.write(i, j, valuef)
                                worksheetra.write(i, j, valuea)
                    except Exception as e:
                        print("Exception: ", e)
        except Exception as e:
            print("Outer Exception: ", e)

        # Capture the time
        date_time = datetime.datetime.utcnow() + datetime.timedelta(
            minutes=330)
        # Capture IST Timezone
        time_isttime_format = date_time.strftime("%H:%M")
        date_isttime_format = date_time.strftime("%d %B")
        # Capture UK Timezone
        date_uktime_format = date_uktime_format.replace(" ", "-")
        date_uktime_format = datetime.datetime.strptime(
            date_uktime_format, '%d-%B')
        date_uktime_format = date_uktime_format.strftime("%d-%B")
        # Fix the width of columns
        worksheetrf.set_column('A:A', 40.00)
        worksheetrf.set_column('B:B', 40.00)
        worksheetrf.set_column('C:C', 3.00)
        # Write the values in the header rows
        worksheetrf.write(0, num_columns + 1, date_isttime_format)
        worksheetrf.write(1, num_columns + 1, time_isttime_format)
        worksheetrf.write(2, num_columns + 1, date_uktime_format)
        worksheetrf.write(3, num_columns + 1, time_uktime_format)
        worksheetrf.write(4, num_columns + 1, 'logSize')
        worksheetrf.write(0, num_columns + 2, date_isttime_format)
        worksheetrf.write(1, num_columns + 2, time_isttime_format)
        worksheetrf.write(2, num_columns + 2, date_uktime_format)
        worksheetrf.write(3, num_columns + 2, time_uktime_format)
        worksheetrf.write(4, num_columns + 2, 'lag')
        # Writing lag and logSize in worksheet
        i = 5
        j = num_columns + 1
        k = num_columns + 2
        for item in currentTotalLogSizeFRAList:
            worksheetrf.write(i, j, item)
            i += 1
        i = 5
        for item in currentTotalLagFRAList:
            worksheetrf.write(i, k, item)
            i += 1
        worksheetrf.write(i, j, currentTotalLogSizeFRA)
        worksheetrf.write(i, k, currentTotalLagFRA)
        # Freeze first column while scrolling to the right
        worksheetrf.freeze_panes(0, 1)
        # Freeze first column while scrolling to the right
        worksheetrf.freeze_panes(0, 2)
        # Freeze first column while scrolling to the right
        worksheetrf.freeze_panes(0, 3)

        # Fix the width of columns
        worksheetra.set_column('A:A', 40.00)
        worksheetra.set_column('B:B', 40.00)
        worksheetra.set_column('C:C', 3.00)
        # Write the values in the header rows
        worksheetra.write(0, num_columns + 1, date_isttime_format)
        worksheetra.write(1, num_columns + 1, time_isttime_format)
        worksheetra.write(2, num_columns + 1, date_uktime_format)
        worksheetra.write(3, num_columns + 1, time_uktime_format)
        worksheetra.write(4, num_columns + 1, 'logSize')
        worksheetra.write(0, num_columns + 2, date_isttime_format)
        worksheetra.write(1, num_columns + 2, time_isttime_format)
        worksheetra.write(2, num_columns + 2, date_uktime_format)
        worksheetra.write(3, num_columns + 2, time_uktime_format)
        worksheetra.write(4, num_columns + 2, 'lag')
        # Writing lag and logSize in worksheet
        i = 5
        j = num_columns + 1
        k = num_columns + 2
        for item in currentTotalLogSizeAMSList:
            worksheetra.write(i, j, item)
            i += 1
        i = 5
        for item in currentTotalLagAMSList:
            worksheetra.write(i, k, item)
            i += 1
        worksheetra.write(i, j, currentTotalLogSizeAMS)
        worksheetra.write(i, k, currentTotalLagAMS)
        # Freeze first column while scrolling to the right
        worksheetra.freeze_panes(0, 1)
        # Freeze first column while scrolling to the right
        worksheetra.freeze_panes(0, 2)
        # Freeze first column while scrolling to the right
        worksheetra.freeze_panes(0, 3)
        workbookr.close()
