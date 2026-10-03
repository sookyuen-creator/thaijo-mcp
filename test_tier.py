"""Offline test for TCI tier checking + search tier filter."""
import asyncio, server as S, tci_tier as T
import test_offline as O  # reuse mock OAI data (runs its own tests on import guard below)

PAGES = {
 # jbm: declares Tier 1 in round 5 (badge alt + Thai text)
 "https://so03.tci-thaijo.org/index.php/jbm/index":
   '<html><body><img src="tci.png" alt="TCI Tier 1"><p>วารสารผ่านการรับรองคุณภาพจากศูนย์ดัชนีการอ้างอิงวารสารไทย (TCI) '
   'กลุ่มที่ 1 ระหว่างปี พ.ศ. 2568-2572</p><p>ISSN 1234-567X (Online)</p></body></html>',
 # old: says Tier 2 but no year -> low confidence
 "https://so03.tci-thaijo.org/index.php/old/index": "<p>Indexed in TCI Tier 2</p>",
 "https://so03.tci-thaijo.org/index.php/old/about": "<p>about page</p>",
 # none: no tier info
 "https://so03.tci-thaijo.org/index.php/none/index": "<p>ยินดีต้อนรับ ISSN 2222-3333</p>",
 "https://so03.tci-thaijo.org/index.php/none/about": "<p>เกี่ยวกับวารสาร กลุ่ม 3 ของนักศึกษา</p>",
}
fetched=[]
async def fake_fetch(url):
    fetched.append(url)
    if url in PAGES: return PAGES[url]
    raise RuntimeError("404 " + url)
S._fetch_text = fake_fetch

async def main():
    out = await S.thaijo_check_tci_tier(S.CheckTierInput(journal_url="https://he02.tci-thaijo.org/index.php/ns"))
    print("[list hit]\n", out, "\n")
    out = await S.thaijo_check_tci_tier(S.CheckTierInput(journal_name="วารสารการบริหารและสังคมศาสตร์ปริทรรศน์"))
    print("[name hit]\n", out.splitlines()[0], "\n")
    out = await S.thaijo_check_tci_tier(S.CheckTierInput(server="so03", journal="jbm"))
    print("[web medium]\n", out, "\n")
    out = await S.thaijo_check_tci_tier(S.CheckTierInput(server="so03", journal="old"))
    print("[web low]\n", out.splitlines()[0], "|", [l for l in out.splitlines() if "ความเชื่อมั่น" in l][0], "\n")
    out = await S.thaijo_check_tci_tier(S.CheckTierInput(server="so03", journal="none"))
    print("[none]\n", out.splitlines()[0], "\n")
    n=len(fetched)
    await S.thaijo_check_tci_tier(S.CheckTierInput(server="so03", journal="jbm"))
    print("cache works:", len(fetched)==n)
    # search with tier filter: mock articles are in journal set 'jbm' (Tier 1)
    out = await S.thaijo_search_articles(S.SearchArticlesInput(server="so03", journal="jbm", keywords=["tiktok"], tci_tiers=[1]))
    print("\n[search tier=1]\n", "\n".join(l for l in out.splitlines() if l.startswith(("##","ขอบเขต","กรอง","(ตรงกับ"))))
    out = await S.thaijo_search_articles(S.SearchArticlesInput(server="so03", journal="jbm", keywords=["tiktok"], tci_tiers=[2]))
    print("\n[search tier=2]\n", "\n".join(l for l in out.splitlines() if l.startswith(("ขอบเขต","กรอง","ยังไม่พบ"))))
asyncio.run(main())
