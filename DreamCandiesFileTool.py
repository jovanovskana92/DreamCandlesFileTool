import csv
import schedule
import time
import logging
import json

# Logging Configuration For Informational Messages
logging.basicConfig(filename='data_extraction.log', level=logging.INFO)

# Reads configuration settings from the JSON configuration file
with open('configuration.json', 'r') as config_file:
    config = json.load(config_file)


def write_csv(file_path, header, data):
    """
    Writes CSV data to the specified file

    Args:
        file_path (str): The path to the CSV file
        header (list): The CSV header
        data (list of lists): The data to write to the CSV file
    """
    with open(file_path, 'w', newline='') as output_file:
        database_writer = csv.writer(output_file)
        database_writer.writerow(header)
        database_writer.writerows(data)


class CustomerDatabase:
    def __init__(self, config):
        """
        Initialize the CustomerDatabase

        Args:
            config (dict): Configuration dictionary containing file paths
        """
        self.sample_file = config['Paths']['customer_sample_file']
        self.database_file = config['Paths']['customer_database_file']
        self.output_file = config['Paths']['output_customer_database_file']
        self._customer_codes = set()

    def load_sample_data(self):
        """Loads the customer sample data from the CSV customer_sample_file into a set"""
        try:
            with open(self.sample_file, 'r', newline='') as sample_file:
                sample_reader = csv.reader(sample_file)
                next(sample_reader)
                for row in sample_reader:
                    customer_code = row[0].strip('"\n\r')
                    self._customer_codes.add(customer_code)
        except (FileNotFoundError, Exception) as e:
            logging.error(f"Error when loading customer sample data: {str(e)}")

    def create_smaller_database(self):
        """Creates a smaller customer database based on the loaded customer codes"""
        try:
            smaller_database = []
            with open(self.database_file, 'r', newline='') as database_file:
                database_reader = csv.reader(database_file)
                header = next(database_reader)

                for row in database_reader:
                    database_customer_code = row[0].split(',')[0].strip('"')
                    if database_customer_code in self._customer_codes:
                        smaller_database.append(row)

            # Uses the write_csv function to write the smaller_customer_database
            write_csv(self.output_file, header, smaller_database)
        except (FileNotFoundError, Exception) as e:
            logging.error(f"Error when creating smaller database: {str(e)}")

    def get_customer_codes(self):
        """Returns the set of customer codes"""
        return self._customer_codes


class InvoiceDatabase:
    def __init__(self, config):
        """
        Initialize the InvoiceDatabase

        Args:
            config (dict): Configuration dictionary containing file paths
        """
        self.invoice_file = config['Paths']['invoice_file']
        self.invoice_item_file = config['Paths']['invoice_item_file']
        self.output_invoice_file = config['Paths']['output_invoice_file']
        self.output_invoice_item_file = config['Paths']['output_invoice_item_file']
        self._invoice_codes = set()

    def load_invoice_data(self, customer_codes):
        """
        Loads invoice data and create a smaller invoice database based on customer codes

        Args:
            customer_codes (set): Set of customer codes
        """
        try:
            smaller_invoice_database = []
            smaller_invoice_item_database = []

            with open(self.invoice_file, 'r', newline='') as invoice_file:
                invoice_reader = csv.reader(invoice_file)
                invoice_header = next(invoice_reader)

                for invoice_row in invoice_reader:
                    invoice_customer_code = invoice_row[0].split(',')[0].strip('"')
                    if invoice_customer_code in customer_codes:
                        self._invoice_codes.add(invoice_row[0].split(',')[1])
                        smaller_invoice_database.append(invoice_row)

            with open(self.invoice_item_file, 'r', newline='') as invoice_item_file:
                invoice_item_reader = csv.reader(invoice_item_file)
                invoice_item_header = next(invoice_item_reader)

                for invoice_item_row in invoice_item_reader:
                    invoice_item_code = invoice_item_row[0].split(',')[0]
                    if invoice_item_code in self._invoice_codes:
                        smaller_invoice_item_database.append(invoice_item_row)

            # Uses the write_csv function to write the smaller_invoice_database
            write_csv(self.output_invoice_file, invoice_header, smaller_invoice_database)

            # Uses the write_csv function to write the smaller_invoice_item_database
            write_csv(self.output_invoice_item_file, invoice_item_header, smaller_invoice_item_database)
        except (FileNotFoundError, Exception) as e:
            logging.error(f"Error when loading invoice data: {str(e)}")

    def get_invoice_codes(self):
        """Returns the set of invoice codes"""
        return self._invoice_codes


def main():
    customer_db = CustomerDatabase(config)
    customer_db.load_sample_data()
    customer_db.create_smaller_database()
    customer_codes = customer_db.get_customer_codes()

    invoice_db = InvoiceDatabase(config)
    invoice_db.load_invoice_data(customer_codes)


if __name__ == "__main__":
    main()

# Schedules the data extraction using the configured schedule time
schedule_time = config['Schedule']['schedule_time']
schedule.every().day.at(schedule_time).do(main)

while True:
    schedule.run_pending()
    time.sleep(1)
