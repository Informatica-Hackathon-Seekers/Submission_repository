"use client";

import { useParams } from "next/navigation";
import { motion } from "framer-motion";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { TrendingUp, TrendingDown, Info, BarChart2, Calendar, Globe, DollarSign } from "lucide-react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
  BarChart,
  Bar,
} from "recharts";

export default function StockDetailsPage() {
  const { symbol } = useParams(); // Get the stock symbol from the URL

  // Retrieve data from sessionStorage
  const storedData = sessionStorage.getItem("stocks-data");
  const stocksData = storedData ? JSON.parse(storedData).data : [];
  const stock = stocksData.find((s: any) => s.symbol === symbol);

  // If stock data is not found, show a fallback or error message
  if (!stock) {
    return (
      <div className="p-6">
        <h1 className="text-2xl font-bold">Stock not found</h1>
        <p className="text-muted-foreground">The stock symbol "{symbol}" is not available.</p>
      </div>
    );
  }

  // Generate chart data dynamically
  const generateChartData = (days: number, up: boolean) => {
    const data = [];
    let baseValue = parseFloat(stock.price); // Use the stock's current price as the base value

    for (let i = 0; i < days; i++) {
      const randomFactor = Math.random() * 5 - (up ? 1.5 : 3);
      baseValue = baseValue + randomFactor;
      data.push({
        date: new Date(Date.now() - (days - i) * 24 * 60 * 60 * 1000).toLocaleDateString(),
        value: baseValue,
        volume: Math.floor(Math.random() * 10000000) + 5000000,
      });
    }

    return data;
  };

  // Mock news data (can be replaced with an API call)
  const getNewsData = (symbol: string) => {
    return [
      {
        id: 1,
        title: `${symbol} Reports Strong Quarterly Earnings, Exceeding Expectations`,
        source: "Financial Times",
        time: "2 hours ago",
        url: "#",
      },
      {
        id: 2,
        title: `Analysts Raise Price Target for ${symbol} Following Product Launch`,
        source: "Bloomberg",
        time: "5 hours ago",
        url: "#",
      },
      {
        id: 3,
        title: `${symbol} Announces New Strategic Partnership to Expand Market Reach`,
        source: "Reuters",
        time: "Yesterday",
        url: "#",
      },
      {
        id: 4,
        title: `Industry Outlook: How ${symbol} is Positioned for Future Growth`,
        source: "Wall Street Journal",
        time: "2 days ago",
        url: "#",
      },
    ];
  };

  // Chart data for different timeframes
  const chartData = {
    "1D": generateChartData(1, stock.up),
    "5D": generateChartData(5, stock.up),
    "1M": generateChartData(30, stock.up),
    "6M": generateChartData(180, stock.up),
    "1Y": generateChartData(365, stock.up),
  };

  return (
    <div className="p-6">
      {/* Stock Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">{stock.name}</h1>
          <p className="text-muted-foreground">{stock.symbol}</p>
        </div>
        <div className="text-right">
          <h2 className="text-3xl font-bold">${stock.price}</h2>
          <p className={`flex items-center ${stock.up ? "text-green-500" : "text-red-500"}`}>
            {stock.up ? <TrendingUp className="h-4 w-4 mr-1" /> : <TrendingDown className="h-4 w-4 mr-1" />}
            {stock.change}
          </p>
        </div>
      </div>

      {/* Tabs for Charts */}
      <Tabs defaultValue="1D" className="mt-6">
        <TabsList>
          <TabsTrigger value="1D">1D</TabsTrigger>
          <TabsTrigger value="5D">5D</TabsTrigger>
          <TabsTrigger value="1M">1M</TabsTrigger>
          <TabsTrigger value="6M">6M</TabsTrigger>
          <TabsTrigger value="1Y">1Y</TabsTrigger>
        </TabsList>
        <TabsContent value="1D">
          <Chart data={chartData["1D"]} />
        </TabsContent>
        <TabsContent value="5D">
          <Chart data={chartData["5D"]} />
        </TabsContent>
        <TabsContent value="1M">
          <Chart data={chartData["1M"]} />
        </TabsContent>
        <TabsContent value="6M">
          <Chart data={chartData["6M"]} />
        </TabsContent>
        <TabsContent value="1Y">
          <Chart data={chartData["1Y"]} />
        </TabsContent>
      </Tabs>

      {/* Stock Details */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mt-6">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>Market Cap</CardTitle>
            <DollarSign className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">${stock.marketCap}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>P/E Ratio</CardTitle>
            <BarChart2 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{stock.peRatio}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>Dividend Yield</CardTitle>
            <Calendar className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{stock.dividend}%</p>
          </CardContent>
        </Card>
      </div>

      {/* News Section */}
      <div className="mt-6">
        <h2 className="text-xl font-bold mb-4">Latest News</h2>
        <div className="space-y-4">
          {getNewsData(symbol as string).map((news) => (
            <Card key={news.id}>
              <CardContent className="p-4">
                <h3 className="font-semibold">{news.title}</h3>
                <p className="text-sm text-muted-foreground">
                  {news.source} · {news.time}
                </p>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
}

// Chart component
const Chart = ({ data }: { data: any[] }) => {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis />
        <Tooltip />
        <Line type="monotone" dataKey="value" stroke="#8884d8" />
      </LineChart>
    </ResponsiveContainer>
  );
};