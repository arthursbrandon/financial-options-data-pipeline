from etl import *

class schwab:

    token_file = '../Algorithmic_trading_system/util/tokens.json'
    

    def __init__(self,api_key,api_secret):
        try:
            self.api_key = api_key
            self.api_secret = api_secret
            self.client = schwabdev.Client(api_key,api_secret,callback_url='https://10.0.0.244:8080',tokens_db=self.token_file)
        except Exception as e:
            print(f'Error: {e}')

    #Schwab Api Market Data Production Calls
    def getQuotes(self,ticker_list):
        """
        Docstring for getQuotes
        
        :param self: Description
        :param ticker_list: Description

        Insert the list of tickers and you will get the cureent quote data for each ticker if applicable.
        """
        try:
            return self.client.quotes(symbols=ticker_list, fields='quote').json()
        except Exception as e:
            print(f'Error: {e}')
    
    def getPriceHist(self,ticker):
        """
        I've set the price_history variable to get daily price data.
        In order for the api call to go through you need a timestamp for the startDate and endDate parameters.

        The startdate I'm using is the current date
        For the end date I chose to use a time date from 4 weeks ago.

        The stamps have to be integers and converted to milliseconds, thats why I'm multiplying them by 1000.
        """

        #Variables for the endDate time stamp
        month = datetime.now().date().month
        day = datetime.now().date().day
        year = datetime.now().date().year

        timesamp = datetime(year=year,month=month,day=day)

        end_timestamp = int(datetime.timestamp(timesamp)*1000)
        start_timestamp = int(datetime.timestamp(timesamp - timedelta(weeks=4))*1000)

        try:
            ticker_price_history = self.client.price_history(
                symbol=ticker,
                periodType='day',
                period=1,
                frequencyType='minute',
                frequency=1,
                startDate = start_timestamp,
                endDate = end_timestamp,
                needExtendedHoursData=True
            ).json()

            return ticker_price_history
        
        except Exception as e:
            print(f'Error: {e}')

    def getOptionschain(self,ticker):
        
        start = datetime.now().date()
        end = start + timedelta(weeks=8)
        
        contracts = self.client.option_chains(
            symbol=ticker,
            contractType = 'ALL',
            strikeCount = 13,
            includeUnderlyingQuote = False,
            strategy = "SINGLE",
            strike = 0.50,
            fromDate=start,
            toDate=end
            
        ).json()
        
        return contracts
        
        





