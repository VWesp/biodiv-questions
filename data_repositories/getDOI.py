import argparse
import requests
import xmltodict
import os
import yaml
import math
import xml.dom.minidom
import sys
import time
import traceback

#command line arguments
parser = argparse.ArgumentParser(description="Retrieve DOI identifiers from oai_dc-XML files through OAI-PMH")
parser.add_argument("-cf", "--config", help="Set path to the config.yaml file", default="config.yaml")
parser.add_argument("-d", "--dir", help="Set path to directory containing the oai_dc-XML folder", required=True)
parser.add_argument("-dp", "--dataportal", help="Specify the dataportal", required=True)
args = parser.parse_args()
try:
    #load config.yaml file
    #throw error if file is not found
    config = None
    if(not os.path.exists(args.config)):
        raise FileNotFoundError("Config file '" + args.config + "' not found.")

    with open(args.config, "r") as config_reader:
         config = yaml.safe_load(config_reader)

    if(not os.path.exists(args.dir)):
        raise FileNotFoundError("Directory '" + args.dir + "' not found.")

    #access the config data and retrieve the URL for the given dataportal
    #throw error if dataportal or metadata format doesn't exist
    dp = args.dataportal
    url = None
    if(not args.dataportal in config.keys()):
        raise KeyError("Dataportal '" + dp + "' not found.")

    url = config[dp] + "&metadataPrefix=oai_dc&identifier="
    #list containing the OAI-PMH and DOI identifiers
    results = []
    #get width of terminal
    width = os.get_terminal_size()[1]
    directory = args.dir
    if(not directory.endswith(os.sep)):
        directory = args.dir + os.sep

    #loop through all files in directory
    for subdir, dirs, files in os.walk(directory + "oai_dc"):
        progress = 0
        print("Progress: [" + " "*width + "] " + "{:.2f}".format(0) + "%", end="\r")
        for file in files:
            path = subdir + os.sep + file
            #access only XML files
            if(file.endswith(".xml")):
                with open(path, "r") as xml_reader:
                    content = xml_reader.read()
                    #retrieve ID
                    id = content.split("<id>")[1].split("</id>")[0]
                    #pangaea only allows 60 requests per minute
                    #to be sure, wait 70 seconds after 60 requests
                    if(dp == "pangaea" and progress % 60 == 0):
                        percentage = float(progress)/len(files)
                        width_percent = math.ceil(width*percentage)
                        for i in range(70, 0, -1):
                            if(progress % 3 == 0):
                                print("Progress: [" + "/"*width_percent + " "*(width-width_percent) + "] - Waiting for " + str(i) + " second(s)", end="\r")
                            elif(progress % 3 == 1):
                                print("Progress: [" + "|"*width_percent + " "*(width-width_percent) + "] - Waiting for " + str(i) + " second(s)", end="\r")
                            else:
                                print("Progress: [" + "\\"*width_percent + " "*(width-width_percent) + "] - Waiting for " + str(i) + " second(s)", end="\r")

                            time.sleep(1)
                            print(" "*len("Progress: [" + " "*width + "] - Waiting for " + str(i) + " second(s)"), end="\r")

                    #request the record
                    request = requests.get(url + id).text
                    try:
                        #transform the requested xml tree to a dictionary
                        content = xmltodict.parse(request.encode("utf-8"))
                        if("metadata" in content["OAI-PMH"]["GetRecord"]["record"].keys()):
                            oai_dc = content["OAI-PMH"]["GetRecord"]["record"]["metadata"]["oai_dc:dc"]
                            identifiers = None
                            if(dp == "zenodo"):
                                identifiers = oai_dc["dc:relation"]
                            else:
                                identifiers = oai_dc["dc:identifier"]

                            doi = None
                            if(isinstance(identifiers, list)):
                                for identifier in identifiers:
                                    if("doi" in identifier):
                                        doi = identifier
                                        break
                            else:
                                doi = identifiers

                            authors = oai_dc["dc:creator"]
                            dates = oai_dc["dc:date"]
                            titles = oai_dc["dc:title"]
                            if(isinstance(authors, list)):
                                authors = ";".join(authors)

                            if(isinstance(dates, list)):
                                dates = dates[0]

                            if(isinstance(titles, list)):
                                titles = titles[0]

                            results.append(id + "\t" + str(doi).strip().replace("\n", " ") +
                                                "\t" + str(authors).strip().replace("\n", " ") +
                                                "\t" + str(dates).strip().replace("\n", " ") +
                                                "\t" + str(titles).strip().replace("\n", " "))
                    except:
                        pass

            #increase the progress by 1 and print it
            progress += 1
            percentage = float(progress)/len(files)
            width_percent = math.ceil(width*percentage)
            if(progress % 3 == 0):
                print("Progress: [" + "/"*width_percent + " "*(width-width_percent) + "] " + "{:.2f}".format(percentage*100) + "%", end="\r")
            elif(progress % 3 == 1):
                print("Progress: [" + "|"*width_percent + " "*(width-width_percent) + "] " + "{:.2f}".format(percentage*100) + "%", end="\r")
            else:
                print("Progress: [" + "\\"*width_percent + " "*(width-width_percent) + "] " + "{:.2f}".format(percentage*100) + "%", end="\r")

        #write the IDs to the CSV files
        with open(directory + "identifiers.tsv", "w", encoding="utf-8") as csv_writer:
            csv_writer.write("OAI-PMH identifier\tDOI\tAuthor(s)\tPublication year\tTitle\n" + u"\n".join(results))

        break
except FileNotFoundError as fnfe:
    print(fnfe)
except KeyError as ke:
    print(ke)
except:
    print(traceback.print_exc())
