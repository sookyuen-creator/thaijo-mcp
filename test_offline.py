"""Offline test: mock ThaiJO OAI responses, exercise every tool."""
import asyncio, server as S

LIST_SETS = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListSets>
<set><setSpec>liberalarts</setSpec><setName>วารสารศิลปศาสตร์ มหาวิทยาลัยธรรมศาสตร์</setName></set>
<set><setSpec>liberalarts:ART</setSpec><setName>บทความวิจัย</setName></set>
<set><setSpec>jbm</setSpec><setName>วารสารบริหารธุรกิจ</setName></set>
</ListSets></OAI-PMH>"""

def rec(i, title_th, title_en, kw, abs_th, year):
    return f"""<record><header><identifier>oai:ojs.so03.tci-thaijo.org:article/{i}</identifier>
<datestamp>{year}-05-01T00:00:00Z</datestamp><setSpec>jbm:ART</setSpec></header>
<metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
<dc:title xml:lang="th-TH">{title_th}</dc:title><dc:title xml:lang="en-US">{title_en}</dc:title>
<dc:creator>สมชาย ใจดี</dc:creator><dc:creator>Jane Doe</dc:creator>
<dc:subject xml:lang="th-TH">{kw}</dc:subject>
<dc:description xml:lang="th-TH">{abs_th}</dc:description>
<dc:description xml:lang="en-US">English abstract {i}</dc:description>
<dc:publisher xml:lang="th-TH">คณะบริหารธุรกิจ</dc:publisher>
<dc:date>{year}-04-30</dc:date><dc:type>info:eu-repo/semantics/article</dc:type>
<dc:identifier>https://so03.tci-thaijo.org/index.php/jbm/article/view/{i}</dc:identifier>
<dc:identifier>10.14456/jbm.{year}.{i}</dc:identifier>
<dc:source xml:lang="th-TH">วารสารบริหารธุรกิจ; ปีที่ 10 ฉบับที่ 1</dc:source><dc:language>tha</dc:language>
</oai_dc:dc></metadata></record>"""

PAGE1 = f"""<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords>
{rec(101,"อิทธิพลของอินฟลูเอนเซอร์ TikTok ต่อความตั้งใจซื้อ","TikTok influencer and purchase intention","TikTok; ความไว้วางใจ; Gen Z","ศึกษาความไว้วางใจและความตั้งใจซื้อสกินแคร์",2024)}
{rec(102,"ภาวะผู้นำการเปลี่ยนแปลง","Transformational leadership","ภาวะผู้นำ; ความผูกพันต่อองค์กร","ศึกษาภาวะผู้นำในหน่วยงานภาครัฐ",2018)}
<resumptionToken completeListSize="3">TOKEN2</resumptionToken></ListRecords></OAI-PMH>"""
PAGE2 = f"""<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords>
{rec(103,"การตลาดดิจิทัลบน TikTok","Digital marketing on TikTok","การตลาดดิจิทัล; TikTok","ศึกษาความตั้งใจซื้อของผู้บริโภค",2022)}
<resumptionToken completeListSize="3"></resumptionToken></ListRecords></OAI-PMH>"""
GET = f"""<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><GetRecord>
{rec(101,"อิทธิพลของอินฟลูเอนเซอร์ TikTok ต่อความตั้งใจซื้อ","TikTok influencer and purchase intention","TikTok; ความไว้วางใจ","บทคัดย่อเต็ม",2024)}</GetRecord></OAI-PMH>"""
ERR = """<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><error code="noRecordsMatch">No matching records</error></OAI-PMH>"""

calls=[]
async def fake(host, params):
    calls.append((host, params))
    v=params["verb"]
    if v=="ListSets": return S._parse_root(LIST_SETS)
    if v=="GetRecord": return S._parse_root(GET)
    if params.get("set")=="empty": return S._parse_root(ERR)
    return S._parse_root(PAGE2 if params.get("resumptionToken")=="TOKEN2" else PAGE1)
S._oai_request = fake

async def main():
    print(await S.thaijo_list_servers()); print("-----")
    print(await S.thaijo_list_journals(S.ListJournalsInput(server="so03", name_filter="บริหาร"))); print("-----")
    print(await S.thaijo_list_articles(S.ListArticlesInput(journal_url="https://so03.tci-thaijo.org/index.php/jbm/index"))); print("-----")
    print(await S.thaijo_search_articles(S.SearchArticlesInput(server="so03", journal="jbm", keywords=["tiktok","ความตั้งใจซื้อ"], match="all", year_from=2020))); print("-----")
    print(await S.thaijo_get_article(S.GetArticleInput(oai_identifier="oai:ojs.so03.tci-thaijo.org:article/101"))); print("-----")
    print(await S.thaijo_list_articles(S.ListArticlesInput(server="so03", journal="empty"))); print("-----")
    try: S.ListJournalsInput(server="xx99")
    except Exception as e: print("validation OK:", str(e).splitlines()[-1][:90])
    print("calls:", calls[2:5])
if __name__ == "__main__":
    asyncio.run(main())
