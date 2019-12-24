from lxml import etree

from defoe.spark_utils import open_stream

def extract_text(element, child_name):
    text = element.xpath(f'//{child_name}/text()')
    if text:
        return ' '.join(text)
    else:
        return None

class Article():

    def __init__(self, file, path):
        self.path = path
        self.document_tree = etree.parse(file)
        self.namespaces = {
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "dc" : "http://purl.org/dc/elements/1.1/",
            "dcterms": "http://purl.org/dc/terms/",
        }

        if not self.single_query('/BL_newspaper/BL_article'):
            raise Exception('Document does not conform to BL article format')

        # <title_metadata>
        #   <title>Reynolds&apos;s Newspaper</title>
        #   <normalisedTitle>Reynolds&apos;s Newspaper</normalisedTitle>
        #   <titleAbbreviation>RDNP</titleAbbreviation>
        #   <changeToTitle>
        #     <name>Reynolds&apos;s Weekly News</name>
        #     <startDate day="05" month="05" year="1850"/>
        #     <endDate day="09" month="02" year="1851"/>
        #   </changeToTitle>
        #   <changeToTitle>
        #     <name>Reynolds&apos;s Newspaper</name>
        #     <startDate day="16" month="02" year="1851"/>
        #     <endDate day="31" month="12" year="1900"/>
        #   </changeToTitle>
        #   <placeOfPublication>London</placeOfPublication>
        #   <datesOfPublication>5 May 1850 - 2 Feb 1851; 9 Feb 1851 - 30 Dec 1900</datesOfPublication>
        #   <typeOfPublication>Newspaper</typeOfPublication>
        #   <subCollection>London Weekly</subCollection>
        # </title_metadata>     
   
        title_metadata = self.single_query('//title_metadata')[0]
        self.title = extract_text(title_metadata, 'title')
        self.normalisedTitle = extract_text(title_metadata, 'normalisedTitle')
        self.titleAbbreviation = extract_text(title_metadata, 'titleAbbreviation')
        self.placeOfPublication = extract_text(title_metadata, 'placeOfPublication')
        self.datesOfPublication = extract_text(title_metadata, 'datesOfPublication')
        self.typeOfPublication = extract_text(title_metadata, 'typeOfPublication')
        self.subCollection = extract_text(title_metadata, 'subCollection')
        self.changeToTitle = title_metadata.xpath('//changeToTitle')

        # <issue_metadata>
        # <volumeNumber></volumeNumber>
        # <issueNumber>1012</issueNumber>
        # <printedDate>SUNDAY, JANUARY 02, 1870</printedDate>
        # <normalisedDate>1870.01.02</normalisedDate>
        # <pageCount>8</pageCount>
        # <reelID>1191</reelID>
        # <qualityRating>Good</qualityRating>
        # </issue_metadata>

        issue_metadata = self.single_query('//issue_metadata')[0]
        self.volumeNumber = extract_text(issue_metadata, 'volumeNumber')
        self.issueNumber = extract_text(issue_metadata, 'issueNumber')
        self.printedDate = extract_text(issue_metadata, 'printedDate')
        self.normalisedDate = extract_text(issue_metadata, 'normalisedDate')
        self.pageCount = extract_text(issue_metadata, 'pageCount')

        # <pageImage>
        # <pageSequence>0001</pageSequence>
        # <pageImageFile>WO1_RDNP_1870_01_02-0001.tif</pageImageFile>
        # <pageCoordinates>407,237,5266,6875</pageCoordinates>
        # <pageSkew>40</pageSkew>
        # </pageImage>

        page_image = self.single_query('//pageImage')[0]
        self.page_sequence = page_image.xpath('//pageSequence/text()')
        self.page_image_file = page_image.xpath('//pageImageFile/text()')
        self.page_coordinates = page_image.xpath('//pageCoordinates/text()')
        self.page_skew = page_image.xpath('//pageSkew/text()')

        # <articleImage>
        # <articleSequence>0001-001</articleSequence>
        # <articleImageFile>WO1_RDNP_1870_01_02-0001-001.tif</articleImageFile>
        # <articleCoordinates>13,687,1666,6660</articleCoordinates>
        # <articleText>
        # <articleWord coord="135,51,325,120">FROM</articleWord>

        article_image = self.single_query('//articleImage')[0]
        self.article_sequence = article_image.xpath('//articleSequence/text()')
        self.article_image_file = article_image.xpath('//articleImageFile/text()')
        self.article_coordinates = article_image.xpath('//articleCoordinates/text()')
        self.article_words = [ArticleWord(w) for w in article_image.xpath('//articleText/articleWord')]

    def __repr__(self):
        return f'''JISC document: 
Title: {self.title}
Normalised Title: {self.normalisedTitle}
Volume number: {self.volumeNumber}
Issue number: {self.issueNumber}
Number of words: {len(self.article_words)}
'''

    def single_query(self, query):
        return self.document_tree.xpath(query, namespaces=self.namespaces)
    
    @staticmethod
    def parse(filename):
        with open_stream(filename) as f:
            return Article(f, filename)


class ArticleWord():
    def __init__(self, element):
        self.coordinates = element.attrib['coord']
        self.word = element.text
    
    def __repr__(self):
        return f'"{self.word}" ({self.coordinates})'
