# -*- coding: utf-8 -*-
"""
Deltahedger for IB papertrading
@author: Jens
"""

from ib_insync import *
from tkinter import *
import pandas as pd
import datetime
import pytz
import threading
import sys
util.patchAsyncio()

conn_type="IBG"

ib = IB()
if conn_type == "TWS":
    ib.connect('127.0.0.1', 7497, clientId=2)
elif conn_type == "IBG":
    ib.connect('127.0.0.1', 4002, clientId=1)
else:
    print("please specify connection type. script exited.")
    sys.exit()

directory = "C:/Users/Jens/AnacondaProjects/IB/ibsync/Deltahedger/"
data_directory = "C:/Users/Jens/AnacondaProjects/IB/ibsync/Deltahedger/datadirectory/"

def load_acc_values():
    net_liq = [v for v in ib.accountValues() if v.tag == 'NetLiquidationByCurrency' and v.currency == 'BASE'][0].value
    exc_liq = [v for v in ib.accountValues() if v.tag == 'ExcessLiquidity' and v.currency == 'EUR'][0].value
    acc_rdy = [v for v in ib.accountValues() if v.tag == 'AccountReady'][0].value
    gross_value = [v for v in ib.accountValues() if v.tag == 'GrossPositionValue' and v.currency == 'EUR'][0].value
    prev_day_eq_w_loan= [v for v in ib.accountValues() if v.tag == 'PreviousDayEquityWithLoanValue' and v.currency == 'EUR'][0].value
    reg_t_margin= [v for v in ib.accountValues() if v.tag == 'RegTMargin' and v.currency == 'EUR'][0].value
    sma= [v for v in ib.accountValues() if v.tag == 'SMA' and v.currency == 'EUR'][0].value
    return {"net_liq": net_liq, "exc_liq": exc_liq, "acc_rdy": acc_rdy,
            "gr_value": gross_value, "prev_d_eq_w_loan": prev_day_eq_w_loan,
            "reg_t_margin": reg_t_margin, "sma": sma}
acc_values = load_acc_values()

# Eliminate pandas error
pd.options.mode.chained_assignment = None
positions=ib.positions()
#Save positions as Pandas df
portfolio=util.df(positions)
# Create greeks df
portfolio_greeks=pd.DataFrame()
#Add necessary columns    
    
aggregated_delta={}
aggregated_gamma={}

target_delta_pd=pd.read_csv(directory + "target_delta.csv")
target_delta_dic={}
hedge_threshold={}

counter=0
for sy in target_delta_pd["symbol"]:
    if sy not in target_delta_dic:
        target_delta_dic[sy]=0
        
    target_delta_dic[sy]= target_delta_pd["target_delta"][counter]
    hedge_threshold[sy]=target_delta_pd["threshold"][counter]
    counter+=1

def active_trading():
    t=datetime.datetime.now()
    if (t.weekday()==5) or (t.weekday()==6):
       print("Its weekend. Get a life.")
       return False
    elif 15 < t.hour >= 22 or (t.minute <30 and t.hour ==15):
       print("No trading hours")
       return False
    else:
        return True

def update_positions():
    global portfolio
    global portfolio_greeks
    counter=0
    
    for sy in target_delta_pd["symbol"]:
        if sy not in target_delta_dic:
            target_delta_dic[sy]=0
        
        target_delta_dic[sy]= target_delta_pd["target_delta"][counter]
        hedge_threshold[sy]=target_delta_pd["threshold"][counter]
        counter+=1
    
    portfolio = portfolio.iloc[0:0]
    portfolio_greeks = portfolio_greeks.iloc[0:0]
    # Checken of req notwendig da zeitintesniver
    positions=ib.reqPositions()
    #Save positions as Pandas df
    portfolio=util.df(positions)
    #Add necessary columns
    portfolio.insert(loc=len(portfolio.columns), column="ticker", value="")
    portfolio.insert(loc=len(portfolio.columns), column="con_details", value="")
    portfolio.insert(loc=len(portfolio.columns), column="market_active", value="")
    portfolio.insert(loc=len(portfolio.columns), column="delta", value="")
    portfolio.insert(loc=len(portfolio.columns), column="share_delta", value="")
    portfolio.insert(loc=len(portfolio.columns), column="ddelta", value="")
    portfolio.insert(loc=len(portfolio.columns), column="gamma", value="")
    portfolio.insert(loc=len(portfolio.columns), column="dgamma", value="")
    portfolio.insert(loc=len(portfolio.columns), column="vega", value="")
    portfolio.insert(loc=len(portfolio.columns), column="dvega", value="")
    portfolio.insert(loc=len(portfolio.columns), column="theta", value="")
    portfolio.insert(loc=len(portfolio.columns), column="dtheta", value="")
    portfolio.insert(loc=len(portfolio.columns), column="implied_vol", value="")        
    portfolio.insert(loc=len(portfolio.columns), column="symbol", value="")
    portfolio.insert(loc=len(portfolio.columns), column="is_trading", value="")
    
    for length in range(len(portfolio)):
        # Add exchange to contracts
        portfolio["contract"][length].exchange="SMART"
        # Request Contract Details and add to portfolio
        portfolio["con_details"][length]=ib.reqContractDetails(portfolio["contract"][length])
        # Add symbol
        portfolio["symbol"][length] = portfolio["contract"][length].symbol
         # Extract trading hours
        eu_time = pytz.timezone('Europe/Amsterdam')
        us_time = pytz.timezone('US/Eastern')
        liq_hours = portfolio["con_details"][length][0].liquidHours.split(';')
        today_hours = liq_hours[0].split(':')[1]
        # Check if contract is trading
        if today_hours == 'CLOSED':
            portfolio["is_trading"][length] = False
        elif today_hours != 'CLOSED':
            now_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
            start = datetime.datetime(year=int(today_hours[:4]), month=int(today_hours[4:6]), day=int(today_hours[6:8]), 
                                   hour= int(today_hours[9:11]), minute = int(today_hours[11:13]),tzinfo=us_time)
            start = start.astimezone(us_time)
            end = datetime.datetime(year=int(today_hours[:4]), month=int(today_hours[4:6]), day=int(today_hours[6:8]), 
                                   hour= int(today_hours[23:25]), minute = int(today_hours[25:27]),tzinfo=us_time)
            end = end.astimezone(us_time)
            if end > now_time > start:
                portfolio["is_trading"][length] = True
            else:
                portfolio["is_trading"][length] = False
        else:
            portfolio["is_trading"][length] = False
            
    portfolio_greeks=pd.DataFrame(index = portfolio["symbol"].unique())
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_delta", value=0.0)
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_ddelta", value=0.0)
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_gamma", value=0.0)
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_dgamma", value=0.0)
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_vega", value=0.0)
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_dvega", value=0.0)
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_theta", value=0.0)
    portfolio_greeks.insert(loc=len(portfolio_greeks.columns), column="aggr_dtheta", value=0.0)
    try:
        portfolio.to_csv(path_or_buf = directory + "portfolio.csv")
    except PermissionError:
        print("Not saved to file. No permission")
        

update_positions()

def load_portfolio_from_csv():
    # Does not worksince contracts are loaded as strings
    global portfolio
    aggregated_delta={}
    counter=0
    # Avoid reqTicker waittime
    portfolio=pd.read_csv(directory + "portfolio.csv") 
    for sy in portfolio["symbol"]:
        if sy not in aggregated_delta:
            aggregated_delta[sy]=0
            
        aggregated_delta[sy]+= portfolio["share_delta"][counter]
        counter+=1

def mid_greek(ticker, greek):
    try:
        midgreek = float(getattr(ticker.modelGreeks,greek))
    except:
        midgreek = (float(getattr(ticker.bidGreeks, greek)) + float(getattr(ticker.askGreeks, greek)))/2
    return midgreek
    
def update_greeks():
    global portfolio
    global portfolio_greeks
    aggregated_delta.clear()
    queue = []
    
    # Request Market data for portfolio positions
    for length in range(len(portfolio)):
        portfolio["ticker"][length] = ib.reqMktData(portfolio["contract"][length], "", True)
        
    for length in range(len(portfolio)):
        counter = 0
        # Check if contract is trading. Else next contract.
        if portfolio["is_trading"][length] == True:
            err = True
            no_data = True
            # Check if market data available. If not wait until 5s then proceed
            # After 10 attempts add ticker to queue and proceed with rest
            while no_data == True:
                try:
                    if portfolio["ticker"][length].contract.secType == "OPT":
                        check = mid_greek(portfolio["ticker"][length],"delta")
                        no_data = False
                    elif portfolio["ticker"][length].contract.secType == "STK":
                        check = portfolio["ticker"][length].open
                        no_data = False
                except AttributeError:
                    counter += 1
                    ib.sleep(0.5)
                    if counter > 10:
                        print("Keine Daten für %s %s empfangen" % portfolio["symbol"][length], portfolio['con_details'][length][0].contract.secType)
                        queue.append(length)
                        break
        elif no_data == True:
            print("%s gequeued" % portfolio["ticker"][length].symbol)
            continue                    
        else:
            continue

        # Determine greeks and add to column
        if portfolio["ticker"][length].contract.secType == "OPT":
            portfolio["delta"][length] = mid_greek(portfolio["ticker"][length],"delta")
            portfolio["share_delta"][length] = mid_greek(portfolio["ticker"][length],"delta")*float(portfolio["ticker"][length].contract.multiplier)*float(portfolio["position"][length])
            portfolio["ddelta"][length] = mid_greek(portfolio["ticker"][length],"delta")*mid_greek(portfolio["ticker"][length],"undPrice")
            portfolio["gamma"][length] = mid_greek(portfolio["ticker"][length],"gamma")
            portfolio["dgamma"][length] = mid_greek(portfolio["ticker"][length],"gamma")*float(portfolio["ddelta"][length])
            portfolio["theta"][length] = mid_greek(portfolio["ticker"][length],"theta")
            portfolio["dtheta"][length] = mid_greek(portfolio["ticker"][length],"theta")*float(portfolio["ticker"][length].contract.multiplier)*float(portfolio["position"][length])
            portfolio["vega"][length] = mid_greek(portfolio["ticker"][length],"vega")
            portfolio["dvega"][length] = mid_greek(portfolio["ticker"][length],"vega")*float(portfolio["ticker"][length].contract.multiplier)*float(portfolio["position"][length])
            portfolio["implied_vol"][length] = mid_greek(portfolio["ticker"][length],"impliedVol")
        elif portfolio["ticker"][length].contract.secType == "STK":
            portfolio["delta"][length] = 1.0
            portfolio["share_delta"][length] = float(portfolio["position"][length])
            portfolio["ddelta"][length] = float(portfolio["position"][length])*((portfolio["ticker"][length].bid+portfolio["ticker"][length].ask)/2)
            portfolio["gamma"][length] = 0.0
            portfolio["dgamma"][length] = 0.0
            portfolio["theta"][length] = 0.0
            portfolio["dtheta"][length] = 0.0
            portfolio["vega"][length] = 0.0
            portfolio["dvega"][length] = 0.0
        else:
            portfolio["share_delta"][length] = 0.0
            portfolio["delta"][length] = 0.0
            portfolio["ddelta"][length] = 0.0
            portfolio["gamma"][length] = 0.0
            portfolio["dgamma"][length] = 0.0
            portfolio["theta"][length] = 0.0
            portfolio["dtheta"][length] = 0.0
            portfolio["vega"][length] = 0.0
            portfolio["dvega"][length] = 0.0
            
    # Write resulting portfolio to csv
    try:
        portfolio.to_csv(path_or_buf = directory + "portfolio.csv")
    except PermissionError:
        print("Not saved to file. No permission")
    # Griechen nach symbol aggregieren 
    counter = 0
    for sy in portfolio["symbol"]:
        if type(portfolio["share_delta"][counter]) is not float:
            pass
        else:
            portfolio_greeks["aggr_delta"][sy] += float(portfolio["share_delta"][counter])
            portfolio_greeks["aggr_ddelta"][sy] += float(portfolio["ddelta"][counter])
            portfolio_greeks["aggr_gamma"][sy] += float(portfolio["gamma"][counter])
            portfolio_greeks["aggr_dgamma"][sy] += float(portfolio["dgamma"][counter])
            portfolio_greeks["aggr_theta"][sy] += float(portfolio["theta"][counter])
            portfolio_greeks["aggr_dtheta"][sy] += float(portfolio["dtheta"][counter])
            portfolio_greeks["aggr_vega"][sy] += float(portfolio["vega"][counter])
            portfolio_greeks["aggr_dvega"][sy] += float(portfolio["dvega"][counter])
            
        counter += 1
    try:
        portfolio_greeks.to_csv(path_or_buf = directory + "portfolio_greeks.csv", header = True, float_format = '%.3f')
    except PermissionError:
        print("Not saved to file. No permission")
  
def create_deltahedges():
    if acc_values["acc_rdy"] == False:
        print("Account not ready for trading. Deltahedge aborted")
        return
    amt_hedges=0
    for row in portfolio_greeks.itertuples():       
        if row.Index in target_delta_dic:
            if target_delta_dic[row.Index]< row.aggr_delta:
                buy_sell="SELL"
            else:
                buy_sell="BUY"
            
            if (abs((row.aggr_delta-target_delta_dic[row.Index])) > hedge_threshold[row.Index]):
                deltahedge(buy_sell,row.Index,row.aggr_delta,target_delta_dic[row.Index])
            else:
                print("No hedge in %s required. Delta is %d while threshold %d" % (row.Index, row.aggr_delta, hedge_threshold[row.Index]))
                
            
                
    order_fulfill()

def deltahedge(buy_sell,symbol,current_delta,target_delta):
    lmt=0
    contract = Stock(symbol, 'SMART', 'USD')
    ib.qualifyContracts(contract)
    con_det = ib.reqContractDetails(contract)
    mktdata = ib.reqMktData(contract, "", True)
    ib.sleep(1)
    amt=abs(round(target_delta-current_delta))
    # Create liquidityadding limitorder
    if buy_sell == "BUY":
        lmt = mktdata.bid
    elif buy_sell == "SELL":
        lmt = mktdata.ask
    # Try to create liquidty adding order (future: if gamma positive)
    
    order = LimitOrder(buy_sell, amt,lmt)
    print(symbol, order)
        
    trade = ib.placeOrder(contract, order)
    return
    #with open("C:/Users/Jens/AnacondaProjects/IB/ibsync/tradelog.txt", "a") as myfile:
     #   myfile.write(str(trade))
     
def order_fulfill():
    # Monitor and amend deltahedge orders until fulfilled or aborted
    counter=1
    orders=ib.openTrades()
    if orders == []:
        print("No open orders")
        return
    else:
        while orders != []:
            for t in orders:
                contract = t.contract
                mktdata = ib.reqMktData(contract, "", True)
                ib.sleep(1)
                # For testing purposes
                print("Changing order attempt %d / 5" % counter)
                # Amend order up to 5 timea
                if counter <= 5:
                    try:
                        if t.order.action == "BUY":
                            t.order.lmtPrice = mktdata.bid
                            ib.placeOrder(contract, t.order)
                        elif t.order.action == "Sell":
                            t.order.lmtPrice = mktdata.ask
                            ib.placeOrder(contract, t.order)
                    except AssertionError:
                        orders=ib.openTrades()
                        continue
                # If spread big cancel orders and wait for next deltahedge
                elif (counter >= 5) and (((mktdata.ask/mktdata.bid)-1) > 0.0005):
                    ib.cancelOrder(t.order)
                    print("Order not fulfilled / not amended / cancelled")
                # If spread small cross spread via market order
                else:
                    new_order = MarketOrder(t.order.action, t.orderStatus.remaining)
                    ib.cancelOrder(t.order)
                    trade = ib.placeOrder(contract, new_order)
                    print("Amended to market order")
                    
                counter += 1            
            # Wait 5 seconds, request openorders and repeat if necessary
            ib.sleep(5)
            # Avoid bugs by exiting after 7 attempts and cancelling order
            if counter > 7:
                ib.cancelOrder(t.order)
                print("Too many attempts -> aborted")
                break
            orders=ib.openTrades()            

def create_chain(underlying,sectype,exchange, *args):
    save = True
    stuff=sectype(underlying,exchange)
    ib.qualifyContracts(stuff)
    ticker = ib.reqTickers(stuff)
    print(ticker)
    uvalue = ticker[0].marketPrice()
    print(uvalue)
    global chains
    chains = ib.reqSecDefOptParams(stuff.symbol, '', stuff.secType, stuff.conId)
    chains = util.df(chains) 
    print(chains)
    if save == True:
        chains.to_csv(path_or_buf = data_directory + underlying + ".csv")
    return chains

def clean_chain(df):
    markprice = 2770
    columns=["expirations","strikes"]
    for c in columns:
        counter1 = 0
        for rows in df[c]:
            str_list=[]    
            for strikes in df[c][counter1]:
                str_list.append(strikes) 
            df[c][counter1]=str_list
                    
            counter1 += 1
            
def testchain():
    create_chain("SPX",Index,"CBOE")
    df=chains
    clean_chain(df)
    
def cr_order():
    # Create order to test / Buy 100 AAPL at limit 101.26 (non-executable)
    order = LimitOrder("BUY", 100, 101.26)
    contract = Stock("AAPL", 'SMART', 'USD')
    trade = ib.placeOrder(contract, order)
    
def t():
    cr_order()
    order_fulfill()
    
def keep_hedging():
    threading.Timer(25.0, keep_hedging).start() # called every 5 minutes
    update_positions()
    update_greeks()
    create_deltahedges()
    
def hedge():
    update_positions()
    ib.sleep(0.1)
    update_greeks()
    ib.sleep(0.1)
    create_deltahedges()

#keep_hedging()
    
#while active_trading() == True:
#    print("do stuff")
        


#limitOrder  = LimitOrder('BUY', 100, 0.05,tif="GTC")
#limitTrade = ib.placeOrder(contract, limitOrder)

