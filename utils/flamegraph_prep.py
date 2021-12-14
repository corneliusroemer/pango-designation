# Data prep
# aws s3 cp  s3://nextstrain-ncov-private/metadata.tsv.gz .
# zcat metadata.tsv.gz | tsv-summarize -H --group-by pango_lineage --count | dos2unix | lineage.tsv
# Need to chop off head

#%%
import csv
from functools import partial
from itertools import accumulate

import pandas as pd

import aliasing

# %%
def cat_join(sep, left, right):
    return left + sep + right

aliasor = aliasing.Aliasor("../pango_designation/alias_key.json")

def lineage_to_stack(lineage):
    if lineage == 'None' or lineage == '':
        return lineage
    split_lin = aliasor.uncompress(lineage).split(".")
    return ";".join(map(aliasor.compress, accumulate(split_lin, partial(cat_join, "."))))

#%%
with open("lineage_count.tsv") as f_in:
    with open("lineage_count_flame.tsv","w") as f_out:
        c = csv.reader(f_in,delimiter="\t")
        w = csv.writer(f_out,delimiter=" ")
        for row in c:
            try:
                w.writerow([lineage_to_stack(row[0]),row[1]])
            except:
                print(row)

