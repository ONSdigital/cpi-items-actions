import pandas as pd

# If you make any edits to unchained.csv in excel and save it, excel messes up the date formats on the columns, to fix that you'll need to run this bit of code.
# look at the current date format and that is what should be in the line below try
# it will then convert it to the right format with the last line. 
un = pd.read_csv('unchained.csv', index_col=0)
columns = {}
for col in un.columns:
    try:
        columns[col] = datetime.strptime(str(col), "%d/%m/%Y").date()
        # columns[col] = datetime.strptime(str(col), "%d/%m/%Y %H:%M").date()
        # columns[col] = datetime.strptime(str(col), "%Y-%m-%d %H:%M:%S").date()
        # columns[col] = datetime.strptime(str(col), "%Y%m").date()
    except ValueError:
        pass
un.rename(columns=columns, inplace=True)
un.to_csv('unchained.csv',date_format='%Y-%m-%d')