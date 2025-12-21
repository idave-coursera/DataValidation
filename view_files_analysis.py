from config import BASE_PATH
SQL_KEYWORDS = [
    "join","inner join","left join","right join","full join","cross join","left outer join","right outer join","full outer join","union","union all","intersect","except","minus","with","exists","in","not in","any","all","group by","having","order by","distinct","count","sum","avg","min","max","over","partition by","row_number","rank","dense_rank","lead","lag","ntile","case","when","then","else","end","coalesce","nullif","decode","explode","posexplode","unnest","lateral view","json_extract","get_json_object","cast","convert","like","ilike","regexp","between","is null","is not null","insert","update","delete","merge","truncate"
]

with open ("./resources/view_files.txt") as f:
    lines = f.readlines()
    linecnt = len(lines)
    non_select_cnt = 0
    for line in lines:
        with open(BASE_PATH+ line.strip()) as f2:
            s =f2.read()
            s = "\n".join([x for x in s.split("\n") if "--" not in x])
            for kw in SQL_KEYWORDS:
                if " " +kw+" " in s or (" " +kw+" " ).upper() in s:
                    print(f"{BASE_PATH+ line.strip()} has {kw}")
                    non_select_cnt+=1
                    break


print(linecnt)
print(non_select_cnt)
