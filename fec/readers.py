import numpy as np
import pandas as pd
from dask import delayed, compute
from pathlib import Path

DATA = Path(__file__).parent.parent.joinpath("data")


def read_cm(year):
    year = str(year)

    cm_header = (pd.read_csv(DATA.joinpath("cm_header.csv"))
                   .columns.map(str.lower))

    cm_dtypes = {
        'cmte_state': "category",
        "cmte_zip": "str",
    }

    cm = pd.read_csv(DATA.joinpath(year, "cm.txt"), sep="|", header=None,
                     index_col=False, names=cm_header,
                     dtype=cm_dtypes)
    return cm


def read_cn(year):
    year = str(year)
    cn_header = (pd.read_csv(DATA.joinpath("cn_header.csv"))
                   .columns.map(str.lower))

    cn_dtypes = {
        "cand_pty_affilitaion": "category",
        "cand_office_st": "category",
        "cand_office": "category",
        "cand_ici": "category",
        "cand_status": "category",
        "cand_stat": "category",
        "cand_zip": "str",
    }

    cn = pd.read_csv(DATA.joinpath(year, "cn.txt"), sep="|", header=None,
                     index_col=False, names=cn_header,
                     dtype=cn_dtypes)
    return cn


def read_ccl(year):
    year = str(year)
    ccl_header = pd.read_csv(DATA.joinpath("ccl_header.csv")).columns.map(str.lower)

    ccl_dtypes = {
        "cmte_tp": "category",
        "cmte_dsgn": "category",
    }

    ccl = pd.read_csv(DATA.joinpath(year, "ccl.txt"), sep="|", header=None,
                      index_col=False, names=ccl_header,
                      dtype=ccl_dtypes)
    return ccl


def read_oth(year):
    year = str(year)
    oth_header = pd.read_csv(DATA.joinpath("oth_header.csv")).columns.map(str.lower)

    oth_dtypes = {
        "amndt_ind": "category",
        "rpt_tp": "category",
        "transaction_pgi": "category",
        "transaction_tp": "category",
        "entity_tp": "category",
        "zip_code": "str",
        "transaction_dt": "str",
        "employer": "str",
        "occupation": "str",
        "other_id": "str",
        "memo_cd": "str",
        "memo_text": "str",
        "transaction_dt": "str",
    }

    oth = pd.read_csv(DATA.joinpath(year, "itoth.txt"), sep="|", header=None,
                      index_col=False, names=oth_header,
                      dtype=oth_dtypes)
    oth['transaction_dt'] = pd.to_datetime(oth['transaction_dt'], format='%m%d%Y',
                                           errors="coerce")
    return oth


def read_pas2(year):
    year = str(year)
    pas2_header = pd.read_csv(DATA.joinpath("pas2_header.csv")).columns.map(str.lower)

    pas2_dtypes = {
        "amndt_ind": "category",
        "rpt_tp": "category",
        "transaction_pgi": "category",
        "transaction_tp": "category",
        "entity_tp": "category",
        "zip_code": "str",
        "memo_cd": "str",
        "memo_text": "str",
        "employer": "str",
        "occupation": "str",
        "transaction_dt": "str",
    }

    pas2 = pd.read_csv(DATA.joinpath(year, "itpas2.txt"), sep="|", header=None,
                       index_col=False, names=pas2_header,
                       dtype=pas2_dtypes)
    pas2['transaction_dt'] = pd.to_datetime(pas2.transaction_dt,
                                            format='%m%d%Y',
                                            errors="coerce")
    return pas2


def read_oppexp(year):
    year = str(year)
    oppexp_header = (pd.read_csv(DATA.joinpath("oppexp_header.csv"))
                       .columns.map(str.lower))

    oppexp_dtypes = {
        "amndt_ind": "category",
        "rpt_tp": "category",
        "form_tp_cd": "category",
        "sched_tp_cd": "category",
        "zip_code": "str",
        "transaction_pgi": "category",
        "category": "category",
        "category_desc": "category",
        "memo_cd": "str",
        "entity_tp": 'category',
        "line_num": "str",
        "transaction_dt": "str",
    }

    oppexp = pd.read_csv(DATA.joinpath(year, "oppexp.txt"), sep="|",
                         header=None,
                         index_col=False, names=oppexp_header,
                         dtype=oppexp_dtypes,
                         encoding="cp1252")
    oppexp['transaction_dt'] = pd.to_datetime(oppexp.transaction_dt,
                                              format='%m%d%Y',
                                              errors='coerce')
    return oppexp


def read_indiv(year):
    year = str(year)
    indiv_header = (pd.read_csv(DATA.joinpath("indiv_header.csv"))
                      .columns.map(str.lower))

    indiv_dtypes = {
        'amndt_ind': 'category',
        'rpt_tp': 'category',
        'transaction_pgi': 'category',
        'transaction_tp': 'category',
        'entity_tp': 'category',
        'zip_code': 'category',
        'state': 'category',
        'memo_cd': 'str',
        'memo_text': 'category',
        "other_id": "str",
        "transaction_dt": "str",
        "image_num": "str",
        "file_num": "float",
    }

    indiv = pd.read_csv(DATA.joinpath(year, "itcont.txt"),
                        sep="|", header=None,
                        index_col=False, names=indiv_header,
                        dtype=indiv_dtypes,
                        encoding="cp1252")
    indiv['transaction_dt'] = pd.to_datetime(indiv.transaction_dt,
                                             format='%m%d%Y',
                                             errors='coerce')
    return indiv


def write(data, kind, year, size=1_000_000):
    gr = data.groupby(np.arange(len(data)) // size)

    for part, v in gr:
        fn = str(DATA.joinpath(f"{kind}-{year}-{part:0>3d}.parquet"))
        v.to_parquet(fn, engine="pyarrow")

        print(f"Wrote {kind}-{year}-{part}", end='\r\n')
    return fn


def do(kind, reader, year):
    df = reader(year)
    return write(df, kind, year)


def main():
    years = ['08', '10', '12', '14', '16']
    kinds = [
        # ('cm', read_cm),
        # ('cn', read_cn),
        # ('ccl', read_ccl),
        # ('oth', read_oth),
        # ('pas2', read_pas2),
        ('oppexp', read_oppexp),
        ('indiv', read_indiv),
    ]
    fns = [delayed(do)(kind, reader, year)
           for (kind, reader) in kinds
           for year in years]
    compute(*fns)


if __name__ == '__main__':
    main()
