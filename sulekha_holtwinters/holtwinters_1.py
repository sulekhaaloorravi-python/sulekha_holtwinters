class holtwinters:
    '''
    author_contact: sulekha.aloorravi@gmail.com
    This class is to forecast timeseries on a Spark Dataframe using Holt winters Forecasting model.

    URL to test this code: https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb
    Example:
        #import package
        from sulekha_holtwinters.holtwinters import holtwinters as hw
        #Pyspark setup
        from pyspark import SparkContext
        from pyspark.sql import SQLContext
        from pyspark.sql import Row, SparkSession
        from pyspark.sql.types import NumericType
        sc = SparkContext.getOrCreate()
        spark = SQLContext(sc)

        #Load data available within this package
        import pkg_resources
        DB_FILE = pkg_resources.resource_filename('sulekha_holtwinters', 'data')
        testDF = spark.read.csv(path = DB_FILE, header = True,inferSchema = True)
    
    '''
    def extract_actuals(sparkDF):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        extract_actuals - Extract observed values from Spark dataframe for further processing
        
        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb
        
        Example:
        extract_actuals(testDF)
        
        '''
        from pyspark import SparkContext
        from pyspark.sql import SQLContext
        from pyspark.sql import Row, SparkSession
        from pyspark.sql.types import NumericType
        sc = SparkContext.getOrCreate()
        spark = SQLContext(sc)
        oldColumns = sparkDF.schema.names
        sparkDF = sparkDF.withColumnRenamed(oldColumns[0], "DateTime").withColumnRenamed(oldColumns[1], "ObservedVals")
        sparkDF.createOrReplaceTempView("timeseries")
        actualDF = spark.sql("select ObservedVals from timeseries")
        observed = [p.ObservedVals for p in actualDF.select("ObservedVals").collect()]
        return observed

    def initiate_trend_factor(sparkDF,L):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        initiate_trend_factor - Function to calculate initial trend factor
        Same for additive and multiplicative models

        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values
        
        L - Seasonal length of dataset

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Example:
        hw.initiate_trend_factor(testDF,24)
        
        '''
        observed = holtwinters.extract_actuals(sparkDF)
        b_numerator = 0.0
        for i in range(L):
            b_numerator += float(observed[L+i] - observed[i])/L
        trendfactor = b_numerator/L
        return trendfactor 

    def initiate_seasonal_indices_multiplicative(sparkDF,L):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        initiate_seasonal_indices_multiplicative - Function to calculate initial values for seasonal components in a multiplicative model

        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values
        
        L - Seasonal length of dataset

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb
        
        Example:
        SeasonalIndices2,Avgs2,Subset2 = hw.initiate_seasonal_indices_multiplicative(testDF,24)
        
        '''
        observed = holtwinters.extract_actuals(sparkDF)
        period = int(len(observed)/L)
        #Average
        Avgs = []
        for i in range(period):
            Avgs.append(sum(observed[L*i:L*i+L])/float(L))
        #Avgs
        #Subset Values according to season length
        Subset = []
        for i in range(period):
            Subset.append(observed[L*i:L*i+L])
        #Subset
        #Indices
        Indices = []
        for j in range(L):
            for i in range(period):
                Indices.append(float(Subset[i][j]/Avgs[i])) #Divide in multiplicative
        #Indices 
        #SeasonalIndices
        SeasonalIndices = []
        for i in range(L):
            SeasonalIndices.append(float(sum(Indices[period*i:period*i+period])/period))
        return SeasonalIndices,Avgs,Subset

    def initiate_seasonal_indices_additive(sparkDF,L):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        initiate_seasonal_indices_additive - Function to calculate initial values for seasonal components in a additive model

        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values
        
        L - Seasonal length of dataset
        
        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb
        
        Example:
        SeasonalIndices,Avgs,Subset = hw.initiate_seasonal_indices_additive(testDF,24)
        
        '''
        observed = holtwinters.extract_actuals(sparkDF)
        period = int(len(observed)/L)
        #Average
        Avgs = []
        for i in range(period):
            Avgs.append(sum(observed[L*i:L*i+L])/float(L))
        #Avgs
        #Subset Values according to season length
        Subset = []
        for i in range(period):
            Subset.append(observed[L*i:L*i+L])
        #Subset
        #Indices
        Indices = []
        for j in range(L):
            for i in range(period):
                Indices.append(float(Subset[i][j]-Avgs[i])) #Subtract in Additive
        #Indices 
        #SeasonalIndices
        SeasonalIndices = []
        for i in range(L):
            SeasonalIndices.append(float(sum(Indices[period*i:period*i+period])/period))
        return SeasonalIndices,Avgs,Subset
        
    def holtwinters_multiplicative(sparkDF,alpha,beta,gamma,L,n_predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        holtwinters_multiplicative - Function to forecast timeseries data by applying Holt Winters Seasonal Forecasting - Multiplicative model

        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values

        alpha - parameter required to calculate Level

        beta - parameter required to calculate Trend

        gamma - parameter required to calculate Seasonality
        
        L - Seasonal length of dataset

        n_predictions - No. of future periods to predict

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb
        
        Example:
        Observed2, Predictions2, Level2, Trend2, Seasonality2 = hw.holtwinters_multiplicative(testDF,0.865,0.01,0.865,24,48)
        
        '''
        
        observed = holtwinters.extract_actuals(sparkDF)
        period = int(len(observed)/L)
        SeasonalIndices,Avgs,Subset = holtwinters.initiate_seasonal_indices_multiplicative(sparkDF,L)
        trendfactor = holtwinters.initiate_trend_factor(sparkDF,L)
        Seasonality = SeasonalIndices
        Predictions = []
        p = len(observed)
        k = len(observed) + n_predictions
        for i in range(k):
            if (i==0):
                Level = [Avgs[0]]
                Trend = [trendfactor]
                Predictions.append(observed[0]) 
                SeasonalityIndex = [Seasonality[0]]
            if (i>0 and i<p):
                Level.append(alpha*(observed[i]/Seasonality[i%L]) + (1-alpha)*(Level[i-1]+Trend[i-1]))
                Trend.append(beta * (Level[i]-Level[i-1]) + (1-beta) * Trend[i-1])
                Seasonality[i%L] = gamma*(observed[i]/Level[i]) + (1-gamma)*Seasonality[i%L]
                Predictions.append(abs(Level[i]+Trend[i]+Seasonality[i%L]))
                SeasonalityIndex.append(Seasonality[i%L])
            if (i>=p):
                m = i-p+1
                Predictions.append(abs((Level[p-1]+m*Trend[p-1])*Seasonality[i%L]))
        return observed, Predictions, Level, Trend, SeasonalityIndex

    def holtwinters_additive(sparkDF,alpha,beta,gamma,L,n_predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        holtwinters_additive - Function to forecast timeseries data by applying Holt Winters Seasonal Forecasting - Additive model

        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values

        alpha - parameter required to calculate Level

        beta - parameter required to calculate Trend

        gamma - parameter required to calculate Seasonality
        
        L - Seasonal length of dataset

        n_predictions - No. of future periods to predict

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb
        
        Example:
        Observed, Predictions, Level, Trend, Seasonality = hw.holtwinters_additive(testDF,0.865,0.01,0.865,24,48)
        
        '''
        observed = holtwinters.extract_actuals(sparkDF)
        period = int(len(observed)/L)
        SeasonalIndices,Avgs,Subset = holtwinters.initiate_seasonal_indices_additive(sparkDF,L)
        trendfactor = holtwinters.initiate_trend_factor(sparkDF,L)
        Seasonality = SeasonalIndices
        Predictions = []
        p = len(observed)
        k = len(observed) + n_predictions
        SeasonalityIndex = []
        for i in range(k):
            if (i==0):
                Level = [Avgs[0]]
                Trend = [trendfactor]
                Predictions.append(observed[0]) 
                SeasonalityIndex = [Seasonality[0]]
            if (i>0 and i<p):
                Level.append(alpha*(observed[i]-Seasonality[i%L]) + (1-alpha)*(Level[i-1]+Trend[i-1]))
                Trend.append(beta * (Level[i]-Level[i-1]) + (1-beta) * Trend[i-1])
                Seasonality[i%L] = gamma*(observed[i]-Level[i]) + (1-gamma)*Seasonality[i%L]
                Predictions.append(abs(Level[i]+Trend[i]+Seasonality[i%L]))
                SeasonalityIndex.append(Seasonality[i%L])
            if (i>=p):
                m = i-p+1
                Predictions.append(abs((Level[p-1]+m*Trend[p-1])+Seasonality[i%L]))
        return observed, Predictions, Level, Trend, SeasonalityIndex

    def Error(Observed, Predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        Error - Function to calculate Holt Winters model Forecasting Error

        Parameters:
        Observed - Observed values [input type: series]
        Predictions - Forecasted values [input type: series]

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Example:
        hw.Error(Observed, Predictions)
        
        '''
        p = len(Observed)
        Error = []
        for i in range(p):
            Error.append(Observed[i] - Predictions[i])
        return sum(Error)
    
    def ABSError(Observed, Predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        ABSError - Function to calculate absolute error from Holt Winters model Forecasting

        Parameters:
        Observed - Observed values [input type: series]
        Predictions - Forecasted values [input type: series]

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Example:
        hw.ABSError(Observed, Predictions)
        
        '''
        p = len(Observed)
        Error = []
        for i in range(p):
            Error.append(abs(Observed[i] - Predictions[i]))
        return sum(Error)
    
    def RMSE(Observed, Predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        RMSE - Function to calculate root mean square error from Holt Winters model Forecasting

        Parameters:
        Observed - Observed values [input type: series]
        Predictions - Forecasted values [input type: series]

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Example:
        hw.RMSE(Observed2, Predictions2)
        
        '''
        import math
        p = len(Observed)
        Error = []
        for i in range(p):
            Error.append(math.pow((Observed[i] - Predictions[i]),2))
        return math.sqrt(sum(Error))
    
    def MAPE(Observed, Predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        MAPE - Function to calculate Mean absolute percentage error from Holt Winters model Forecasting

        Parameters:
        Observed - Observed values [input type: series]
        Predictions - Forecasted values [input type: series]

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Example:
        hw.MAPE(Observed, Predictions)
        
        '''
        n = len(Observed)
        MAPE = []
        for i in range(n):
            MAPE.append(abs((Observed[i] - Predictions[i])/Observed[i]))
        return sum(MAPE)*100/n
    
    def Accuracy(Observed, Predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        Accuracy - Function to calculate forecasting accuracy from Holt Winters model Forecasting

        Parameters:
        Observed - Observed values [input type: series]
        Predictions - Forecasted values [input type: series]

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Example:
        hw.Accuracy(Observed, Predictions)
        
        '''
        Acc = []
        n = len(Observed)
        for i in range(n):
            Acc.append((1-abs((Observed[i] - Predictions[i]))/Observed[i])*100)
        return sum(Acc)/n      
    
    def BestFit_Multiplicative(sparkDF, interval, denominator, L, n_predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        BestFit_Multiplicative - Function to calculate alpha, beta and gamma values that gives best Forecasting accuracy

        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values

        interval - intervals of alpha, beta, gamma parameters to be passed.

        denominator - denominator for alpha, beta, gamma parameters
        
        Example: interval of 15 and denominator of 100 generates parameters in the series [0.0, 0.15, 0.3, o.45, 0.6...]
        Caution: Choose the interval and denominator wisely to minimize computational cost

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Usage: hw.BestFit_Multiplicative(testDF, interval=865, denominator=1000,L = 24,n_predictions = 36)
        
        '''
        Acc = []
        alpha = []
        beta = []
        gamma = []
        for i in [float(j) / denominator for j in range(0, denominator, interval)]:
            for a in [float(b) / denominator for b in range(0, denominator, interval)]:
                for l in [float(m) / denominator for m in range(0, denominator, interval)]:
                    Observed, Predictions, Level, Trend, Seasonality = holtwinters.holtwinters_multiplicative(sparkDF,i,a,l,L,n_predictions)
                    Acc.append(holtwinters.Accuracy(Observed, Predictions))
                    alpha.append(i)
                    beta.append(a)
                    gamma.append(l)
        ind = Acc.index(max(Acc))
        return max(Acc), alpha[ind], beta[ind], gamma[ind]

    def BestFit_Additive(sparkDF, interval, denominator, L, n_predictions):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        BestFit_Additive - Function to calculate alpha, beta and gamma values that gives best Forecasting accuracy

        Parameters:
        sparkDF - Input a spark dataframe with only two columns - Date/Time and Observed values

        interval - intervals of alpha, beta, gamma parameters to be passed.

        denominator - denominator for alpha, beta, gamma parameters
        
        Example: interval of 15 and denominator of 100 generates parameters in the series [0.0, 0.15, 0.3, o.45, 0.6...]
        Caution: Choose the interval and denominator wisely to minimize computational cost

        Usage: hw.BestFit_Additive(testDF, interval=865, denominator=1000,L = 24,n_predictions = 36)
        
        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb
        
        '''
        Acc = []
        alpha = []
        beta = []
        gamma = []
        for i in [float(j) / denominator for j in range(0, denominator, interval)]:
            for a in [float(b) / denominator for b in range(0, denominator, interval)]:
                for l in [float(m) / denominator for m in range(0, denominator, interval)]:
                    Observed, Predictions, Level, Trend, Seasonality = holtwinters.holtwinters_additive(sparkDF,i,a,l,L,n_predictions)
                    Acc.append(holtwinters.Accuracy(Observed, Predictions))
                    alpha.append(i)
                    beta.append(a)
                    gamma.append(l)
        ind = Acc.index(max(Acc))
        return max(Acc), alpha[ind], beta[ind], gamma[ind]
    
    def CreateGraphs(model):
        '''
        author_contact: sulekha.aloorravi@gmail.com
        CreateGraphs - Function to plot Observed, Predictions, Level, Trend, Seasonality values as interactive line graphs

        Parameters:
        model - holtwinters_additive() or holtwinters_multiplicative() model

        URL to load data available in this package and test this code:
        https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

        Example:
        model1 = hw.holtwinters_additive(testDF,0.865,0.01,0.865,24,48)
        hw.CreateGraphs(model1)

        model2 = hw.holtwinters_multiplicative(testDF,0.865,0.01,0.865,24,48)
        hw.CreateGraphs(model2)      
        
        '''
        #Plotly libraries used in this class
        from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
        from plotly import tools
        init_notebook_mode(connected=True) #Plotly offline
        from plotly import graph_objs as go
        from plotly import figure_factory

        #input
        Observed, Predictions, Level, Trend, Seasonality = model
    
        observed = go.Scatter(y = Observed)
        predictions = go.Scatter(y = Predictions)
        level = go.Scatter(y = Level)
        trend = go.Scatter(y = Trend)
        seasonality = go.Scatter(y = Seasonality)

        data = [observed, predictions, level, trend, seasonality]
        fig = tools.make_subplots(rows=5, cols=1, subplot_titles = ("Observed","Predictions","Level","Trend","Seasonality"),print_grid=False)
        fig.append_trace(observed, 1, 1)
        fig.append_trace(predictions, 2, 1)
        fig.append_trace(level, 3, 1)
        fig.append_trace(trend, 4, 1)
        fig.append_trace(seasonality, 5, 1)

        fig['layout'].update(height=600, width=900, title='Holt Winters Plot', showlegend=False)
        iplot(fig)    
