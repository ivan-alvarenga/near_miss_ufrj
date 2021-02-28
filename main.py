import src.search_cids as sc


if __name__ == '__main__':

    path = './data/codigos.csv'

    df = sc.read_codes(path)

    print(df.head())