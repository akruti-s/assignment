import xml.etree.ElementTree as ET
import requests
import logging
#SAVING XML
def saving_xml(URL):
    '''Saving_xml function is to url as input and then load xml file from that url
    and return file name of xml file downloaded as output'''
    response=requests.get(URL)
    with open('sample.xml','wb')as file:
       file.write(response.content)
    logging.info("Xml file saved successfully")
    return('sample.xml')
#parsing xml and saving zip file
def parsexml(file_n):
    '''parsexml function is used to parse the xml file given as parameter
    and downloadthe zip file from first download link present in it'''
    root_node=ET.parse(file_n).getroot()
    for elem in root_node.findall('result/doc/str'):
        if elem.attrib['name']=="download_link":
            byteData=elem.text
            break
    import wget
    wget.download(byteData,'sam.zip')
    logging.info("zip file downloaded succcessfully")
    return('sam.zip')
#extracting content from zip file
def extractzip(filename):
    from zipfile import ZipFile
    with ZipFile(filename,'r') as zip:
        zip.extractall()
    logging.info("Xml is extracted from zip successfully")    
#converting xml to csv
def xml_to_csv():
    root=ET.parse("DLTINS_20210117_01of01.xml").getroot()
    import pandas as pd
    cols = ["FinINstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNM", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy","Issr"]
    rows = []
    for i in root.findall("FinInstrmGnlAttrbts"):
       id = i.find("Id").text
       Fullnm = i.find("FullNm").text
       clssfctntp = i.find("ClssfctnTp").text
       cmmdtyderivind = i.find("CmmdthDerivInd").text
       ntnlccy = i.find("NtnlCcy").text
       Issr=i.find("Issr").text
       rows.append({"FinINstrmGnlAttrbts.Id": id,
                 "FinInstrmGnlAttrbts.FullNM": Fullnm,
                 "FinInstrmGnlAttrbts.ClssfctnTp": clssfctntp,
                 "FinInstrmGnlAttrbts.CmmdtyDerivInd": cmmdtyderivind,
                 "FinInstrmGnlAttrbts.NtnlCcy": ntnlccy,
                 "Issr":Issr})
    df = pd.DataFrame(rows, columns=cols)
# Writing dataframe to csv
    df.to_csv('data.csv')
#Store  csv in a s3 bucket
def store_csv():
    import boto
    import boto3.s3.key
    bucket=aws_connection.get_bucket('mybucket')
    k=key(bucket)
    k.key='output.csv'
    k.set_contents_from_filename('data.csv')
    print(777)

def main():
    url='https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19t23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'
    file_name=saving_xml(url)
    Zip_file_name=parsexml(file_name)
    extractzip(Zip_file_name)
    xml_to_csv()
    #store_csv()
if __name__=="__main__":
    main()
