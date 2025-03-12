"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { Search, TrendingUp, TrendingDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { useRouter } from "next/navigation";
import MarketIndicesSection from "@/components/marketIndicesSection";
import Link from "next/link";
import axios from "axios";
import { useUserPreferences } from "@/contexts/userPreferenceContext";

type StockData = {
  symbol: string;
  name: string;
  price: string;
  change: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  up: boolean;
};

type AssetType = "stocks" | "forex" | "crypto" | "etf";

// Top 8 stocks for the slider
const TOP_SLIDER_STOCKS = ["NVDA", "TSLA", "AMZN", "PLTR", "AMD", "AVGO", "SMCI", "AAPL"];

export default function StockAnalysisPage() {
  const [searchQuery, setSearchQuery] = useState("");
  const [assetType, setAssetType] = useState<AssetType>("stocks");
  const [stocks, setStocks] = useState<StockData[]>([]);
  const [loading, setLoading] = useState(true);
  const [slidingStocks, setSlidingStocks] = useState<StockData[]>([]);
  const router = useRouter();

  const { preferences } = useUserPreferences();
  const { topStocks } = preferences;
  const API_KEY = process.env.NEXT_PUBLIC_TWELVE_DATA_API_KEY_1;//076190d888d84f25b4cda476e7d45026

  

  const fetchStockData = async (symbols: string[], type: AssetType): Promise<StockData[]> => {
    const requests = symbols.map((symbol) =>
      axios.get(`https://api.twelvedata.com/time_series`, {
        params: {
          symbol,
          interval: "1min",
          apikey: API_KEY,
          outputsize: 1,
        },
      })
    );

    const responses = await Promise.all(requests);
    return responses.map((res, index) => {
      if (!res.data?.values?.[0]) {
        console.error("Unexpected API response structure:", res.data);
        return createFallbackStock(symbols[index], type);
      }

      const values = res.data.values[0];
      return createStockData(symbols[index], type, values);
    });
  };

  const createFallbackStock = (symbol: string, type: AssetType): StockData => ({
    symbol,
    name: getAssetName(symbol, type),
    price: "N/A",
    change: "N/A",
    open: "N/A",
    high: "N/A",
    low: "N/A",
    close: "N/A",
    volume: "N/A",
    up: false,
  });

  const createStockData = (symbol: string, type: AssetType, values: any): StockData => ({
    symbol,
    name: getAssetName(symbol, type),
    price: values.close,
    change: calculateChange(values.open, values.close),
    open: values.open,
    high: values.high,
    low: values.low,
    close: values.close,
    volume: values.volume,
    up: parseFloat(values.close) > parseFloat(values.open),
  });

  const fetchAndStoreData = async (type: AssetType) => {
    setLoading(true);
    try {
      if (type === "stocks") {
        // Combine and deduplicate symbols
        
        const userSymbols: string[] = Array.isArray(topStocks) ? topStocks : [];
        const sliderSymbols: string[] = TOP_SLIDER_STOCKS;
        const combinedSymbols = [...userSymbols, ...sliderSymbols];
        const uniqueSymbols = combinedSymbols.filter((value, index, self) => {
            return self.indexOf(value) === index;
          });
        
        // Now use the deduplicated array
        const allSymbols: string[] = uniqueSymbols;

        // Fetch all data at once
        const allStockData = await fetchStockData(allSymbols, type);

        // Separate the data
        const userStocks = allStockData.filter(stock => userSymbols.includes(stock.symbol));
        const sliderStocks = allStockData.filter(stock => sliderSymbols.includes(stock.symbol));

        // Store separately with timestamps
        sessionStorage.setItem("stocks-data", JSON.stringify({
          data: userStocks,
          timestamp: Date.now()
        }));

        sessionStorage.setItem("slider-stocks", JSON.stringify({
          data: sliderStocks,
          timestamp: Date.now()
        }));

        setStocks(userStocks);
        setSlidingStocks(sliderStocks);
      } else {
        // Handle other asset types normally
        const symbols = getSymbolsByType(type);
        const stockData = await fetchStockData(symbols, type);
        
        sessionStorage.setItem(`${type}-data`, JSON.stringify({
          data: stockData,
          timestamp: Date.now()
        }));

        setStocks(stockData);
      }
    } catch (error) {
      console.error("Error fetching data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const loadData = async () => {
      if (assetType === "stocks") {
        // Check both storage locations for stocks
        const [stocksStorage, sliderStorage] = await Promise.all([
          sessionStorage.getItem("stocks-data"),
          sessionStorage.getItem("slider-stocks"),
        ]);

        const oneHour = 60 * 60 * 1000;
        const now = Date.now();

        // Check if both datasets are valid
        if (stocksStorage && sliderStorage) {
          const { data: userStocks, timestamp: userTime } = JSON.parse(stocksStorage);
          const { data: sliderStocks, timestamp: sliderTime } = JSON.parse(sliderStorage);

          if ((now - userTime < oneHour) && (now - sliderTime < oneHour)) {
            setStocks(userStocks);
            setSlidingStocks(sliderStocks);
            setLoading(false);
            return;
          }
        }
      } else {
        // Handle other asset types
        const storedData = sessionStorage.getItem(`${assetType}-data`);
        if (storedData) {
          const { data, timestamp } = JSON.parse(storedData);
          if (Date.now() - timestamp < 60 * 60 * 1000) {
            setStocks(data);
            setLoading(false);
            return;
          }
        }
      }

      // If any data is invalid or missing, fetch fresh
      fetchAndStoreData(assetType);
    };

    loadData();
  }, [assetType]);

  // Calculate percentage change
  const calculateChange = (open: string, close: string): string => {
    const openPrice = parseFloat(open);
    const closePrice = parseFloat(close);
    const change = ((closePrice - openPrice) / openPrice) * 100;
    return `${change.toFixed(2)}%`;
  };

  // Get symbols based on asset type
  const getSymbolsByType = (type: AssetType): string[] => {
    switch (type) {
      case "stocks":
        return topStocks || []; // Use the user's preferred stocks
      case "forex":
        return ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD"];
      case "crypto":
        return ["BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD"];
      case "etf":
        return ["SPY", "QQQ", "VOO", "ARKK"];
      default:
        return [];
    }
  };

  // Get asset name based on symbol and type
  const getAssetName = (symbol: string, type: AssetType): string => {
    switch (type) {
      case "stocks":
        return symbol; // Replace with actual names if needed
      case "forex":
        return symbol;
      case "crypto":
        return symbol;
      case "etf":
        return symbol;
      default:
        return symbol;
    }
  };

  // Handle search
  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      router.push(`/stock-analysis/${searchQuery.trim()}`);
    }
  };

  return (
    <div className="flex flex-col">
      {/* Market Indices */}
      <MarketIndicesSection />
  
      {/* Asset Type Selector */}
      <div className="flex justify-center mt-8 space-x-4">
        {(["stocks", "forex", "crypto", "etf"] as AssetType[]).map((type) => (
          <Button
            key={type}
            variant={assetType === type ? "default" : "outline"}
            onClick={() => setAssetType(type)}
          >
            {type.toUpperCase()}
          </Button>
        ))}
      </div>
  
      {/* Search Box */}
      <section className="w-full py-12 md:py-24 bg-gradient-to-b from-background to-muted/20">
        <div className="container px-4 md:px-6">
          <motion.div
            className="mx-auto max-w-3xl text-center"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
          >
            <h1 className="text-3xl font-bold tracking-tighter sm:text-4xl md:text-5xl">
              {assetType.toUpperCase()} Analysis
            </h1>
            <p className="mt-4 text-muted-foreground md:text-xl">
              Search for any {assetType} to get detailed analysis and insights
            </p>
          </motion.div>
          <motion.div
            className="mx-auto mt-8 max-w-2xl"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2, duration: 0.5 }}
          >
            <form onSubmit={handleSearch} className="relative">
              <Input
                type="text"
                placeholder={`Search for a ${assetType} (e.g., ${
                  assetType === "stocks"
                    ? "AAPL"
                    : assetType === "forex"
                    ? "EUR/USD"
                    : assetType === "crypto"
                    ? "BTC/USD"
                    : "SPY"
                })`}
                className="h-14 rounded-full pl-6 pr-14 text-lg"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
              <Button
                type="submit"
                size="icon"
                className="absolute right-2 top-2 h-10 w-10 rounded-full"
              >
                <Search className="h-5 w-5" />
              </Button>
            </form>
          </motion.div>
        </div>
      </section>

      {/* Sliding Moving Navs */}
      <section className="w-full overflow-hidden py-6 bg-card">
        <div className="relative">
          <div className="mb-8 text-center">
            <h2 className="text-3xl font-bold">Top Market Performers</h2>
            <p className="mt-2 text-muted-foreground">
              Trending stocks in the market today
            </p>
          </div>
          {/* Top Nav - Moving Left */}
          <motion.div
            initial={{ x: "0%" }}
            animate={{ x: "-50%" }}
            transition={{
              repeat: Infinity,
              duration: 30,
              ease: "linear",
            }}
            className="flex w-max space-x-8 px-4"
          >
            {[...slidingStocks, ...slidingStocks].map((stock, i) => (
              <Link
                href={`/stock-analysis/${stock.symbol}`}
                key={`top-${i}`}
                className="flex items-center space-x-3 rounded-lg border bg-card p-3 shadow-sm hover:bg-accent/50"
              >
                <div
                  className={`flex h-10 w-10 items-center justify-center rounded-full ${
                    stock.up ? "bg-green-100" : "bg-red-100"
                  } ${
                    stock.up ? "text-green-600" : "text-red-600"
                  } dark:bg-opacity-20`}
                >
                  {stock.up ? (
                    <TrendingUp className="h-5 w-5" />
                  ) : (
                    <TrendingDown className="h-5 w-5" />
                  )}
                </div>
                <div>
                  <div className="flex items-center space-x-2">
                    <span className="font-medium">{stock.name}</span>
                    <span className="text-xs text-muted-foreground">
                      {stock.symbol}
                    </span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <span>{stock.price}</span>
                    <span
                      className={`text-sm ${
                        stock.up ? "text-green-500" : "text-red-500"
                      }`}
                    >
                      {stock.change}
                    </span>
                  </div>
                </div>
              </Link>
            ))}
          </motion.div>

          {/* Bottom Nav - Moving Right */}
          <motion.div
            initial={{ x: "-50%" }}
            animate={{ x: "0%" }}
            transition={{
              repeat: Infinity,
              duration: 30,
              ease: "linear",
            }}
            className="mt-4 flex w-max space-x-8 px-4"
          >
            {[...slidingStocks.reverse(), ...slidingStocks.reverse()].map(
              (stock, i) => (
                <Link
                  href={`/stock-analysis/${stock.symbol}`}
                  key={`bottom-${i}`}
                  className="flex items-center space-x-3 rounded-lg border bg-card p-3 shadow-sm hover:bg-accent/50"
                >
                  <div
                    className={`flex h-10 w-10 items-center justify-center rounded-full ${
                      stock.up ? "bg-green-100" : "bg-red-100"
                    } ${
                      stock.up ? "text-green-600" : "text-red-600"
                    } dark:bg-opacity-20`}
                  >
                    {stock.up ? (
                      <TrendingUp className="h-5 w-5" />
                    ) : (
                      <TrendingDown className="h-5 w-5" />
                    )}
                  </div>
                  <div>
                    <div className="flex items-center space-x-2">
                      <span className="font-medium">{stock.name}</span>
                      <span className="text-xs text-muted-foreground">
                        {stock.symbol}
                      </span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span>{stock.price}</span>
                      <span
                        className={`text-sm ${
                          stock.up ? "text-green-500" : "text-red-500"
                        }`}
                      >
                        {stock.change}
                      </span>
                    </div>
                  </div>
                </Link>
              )
            )}
          </motion.div>
        </div>
      </section>
  
      {/* Display Fetched Data */}
      <section className="w-full py-12 md:py-16 bg-background">
        <div className="container px-4 md:px-6">
        <div className="mb-8 text-center">
            <h2 className="text-3xl font-bold">
              {assetType === "stocks" ? "Your Picks" : `Today's ${assetType.toUpperCase()}`}
            </h2>
            <p className="mt-2 text-muted-foreground">
              {assetType === "stocks" ? "Your chosen stocks at a glance" : `Top performing ${assetType} in the market today`}
            </p>
          </div>

          {loading ? (
            <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
              {[...Array(6)].map((_, i) => (
                <Card key={i} className="animate-pulse">
                  <CardContent className="p-6">
                    <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
                    <div className="h-4 bg-muted rounded w-1/2 mb-2"></div>
                    <div className="h-4 bg-muted rounded w-1/2 mb-4"></div>
                    <div className="h-8 bg-muted rounded w-full mb-4"></div>
                    <div className="grid grid-cols-2 gap-2">
                      <div className="h-4 bg-muted rounded"></div>
                      <div className="h-4 bg-muted rounded"></div>
                      <div className="h-4 bg-muted rounded"></div>
                      <div className="h-4 bg-muted rounded"></div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          ) : (
            <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
              {stocks.map((stock, i) => (
                <motion.div
                  key={i}
                  initial={{ opacity: 0, y: 20 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  transition={{ delay: i * 0.1, duration: 0.5 }}
                  viewport={{ once: true }}
                  whileHover={{ y: -5, transition: { duration: 0.2 } }}
                >
                  <Link href={`/stock-analysis/${stock.symbol}`}>
                    <Card className="overflow-hidden hover:shadow-lg transition-shadow">
                      <CardContent className="p-0">
                        <div className="p-6">
                          <div className="flex items-center justify-between">
                            <div>
                              <h3 className="font-bold">{stock.name}</h3>
                              <p className="text-sm text-muted-foreground">
                                {stock.symbol}
                              </p>
                            </div>
                            <div
                              className={`flex items-center rounded-full px-2 py-1 text-xs font-medium ${
                                stock.up
                                  ? "bg-green-100 text-green-600"
                                  : "bg-red-100 text-red-600"
                              } dark:bg-opacity-20`}
                            >
                              {stock.up ? (
                                <TrendingUp className="mr-1 h-3 w-3" />
                              ) : (
                                <TrendingDown className="mr-1 h-3 w-3" />
                              )}
                              {stock.change}
                            </div>
                          </div>
                          <div className="mt-4">
                            <div className="flex justify-between">
                              <span className="text-2xl font-bold">
                                {stock.price}
                              </span>
                            </div>
                            <div className="mt-2 grid grid-cols-2 gap-2 text-sm">
                              <div>
                                <p className="text-muted-foreground">Open</p>
                                <p className="font-medium">{stock.open}</p>
                              </div>
                              <div>
                                <p className="text-muted-foreground">High</p>
                                <p className="font-medium">{stock.high}</p>
                              </div>
                              <div>
                                <p className="text-muted-foreground">Low</p>
                                <p className="font-medium">{stock.low}</p>
                              </div>
                              <div>
                                <p className="text-muted-foreground">Volume</p>
                                <p className="font-medium">{stock.volume}</p>
                              </div>
                            </div>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  </Link>
                </motion.div>
              ))}
            </div>
          )}
        </div>
      </section>
    </div>
  );
}