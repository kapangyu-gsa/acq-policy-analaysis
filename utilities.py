import pandas as pd
import io
import requests

def load_contract_opportunities_archived_data(years: []) -> pd.DataFrame:
    """
    Takes in a list of 4-digit years, returns a Pandas datafame
    of the archived contract opportunities for those years.
    """
    
    dfs = []
    for year in years:
        fileURL = f"https://beta.sam.gov/api/prod/fileextractservices/v1/api/download/Contract%20Opportunities/Archived%20Data/FY{year}_archived_opportunities.csv?privacy=Public"
        response = requests.get(fileURL)
        if (response.ok):
            fileContent = response.content
            df = pd.read_csv(io.StringIO(fileContent.decode('iso-8859-1')), low_memory= False)
            dfs.append(df)
        else:
            print(f"WARNING: File FY{year}_archived_opportunities.csv is not found.")
        
    return pd.concat(dfs, ignore_index = True) if (len(dfs) > 0) else pd.DataFrame()


if __name__ == "__main__":
    df = load_contract_opportunities_archived_data([1970])
    print(df.head())