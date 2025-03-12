"use client";

import { motion } from "framer-motion";
import Link from "next/link";
import Image from "next/image";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ArrowRight, TrendingUp, BarChart2, LineChart, PieChart } from "lucide-react";
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip } from "recharts";

export default function TrendAnalysisPage() {
  const container = {
    hidden: { opacity: 0 },
    show: {
      opacity: 1,
      transition: {
        staggerChildren: 0.1,
      },
    },
  };

  const item = {
    hidden: { opacity: 0, y: 20 },
    show: { opacity: 1, y: 0 },
  };

  // Sample data for charts
  const marketTrendData = [
    { name: "Jan", value: 4000 },
    { name: "Feb", value: 3000 },
    { name: "Mar", value: 5000 },
    { name: "Apr", value: 2780 },
    { name: "May", value: 1890 },
    { name: "Jun", value: 2390 },
    { name: "Jul", value: 3490 },
    { name: "Aug", value: 4000 },
    { name: "Sep", value: 5000 },
    { name: "Oct", value: 6000 },
    { name: "Nov", value: 7000 },
    { name: "Dec", value: 8000 },
  ];

  const trendCategories = [
    {
      title: "Market Trends",
      description: "Analyze overall market movements and sector performance",
      icon: LineChart,
      color: "bg-blue-100 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400",
    },
    {
      title: "Industry Analysis",
      description: "Deep dive into specific industry trends and forecasts",
      icon: BarChart2,
      color: "bg-purple-100 text-purple-600 dark:bg-purple-900/30 dark:text-purple-400",
    },
    {
      title: "Economic Indicators",
      description: "Track key economic metrics and their impact on markets",
      icon: TrendingUp,
      color: "bg-green-100 text-green-600 dark:bg-green-900/30 dark:text-green-400",
    },
    {
      title: "Sector Rotation",
      description: "Identify shifting capital flows between market sectors",
      icon: PieChart,
      color: "bg-orange-100 text-orange-600 dark:bg-orange-900/30 dark:text-orange-400",
    },
  ];

  const trendArticles = [
    {
      title: "The Rise of AI in Financial Markets",
      description: "How artificial intelligence is transforming trading strategies and market analysis.",
      image: "https://images.unsplash.com/photo-1639322537228-f710d846310a?q=80&w=500&auto=format&fit=crop",
      category: "Technology",
      date: "May 15, 2025",
    },
    {
      title: "ESG Investing: The New Normal",
      description: "Environmental, Social, and Governance factors are increasingly driving investment decisions.",
      image: "https://images.unsplash.com/photo-1497435334941-8c899ee9e8e9?q=80&w=500&auto=format&fit=crop",
      category: "Sustainability",
      date: "May 12, 2025",
    },
    {
      title: "Cryptocurrency Market Maturation",
      description: "How the crypto market is evolving from speculative assets to mainstream financial instruments.",
      image: "https://images.unsplash.com/photo-1621761191319-c6fb62004040?q=80&w=500&auto=format&fit=crop",
      category: "Cryptocurrency",
      date: "May 10, 2025",
    },
    {
      title: "Supply Chain Resilience in Global Markets",
      description: "Companies are rethinking supply chains after recent global disruptions.",
      image: "https://images.unsplash.com/photo-1586528116311-ad8dd3c8310d?q=80&w=500&auto=format&fit=crop",
      category: "Global Economy",
      date: "May 8, 2025",
    },
    {
      title: "The Future of Remote Work and Its Economic Impact",
      description: "How changing work patterns are affecting real estate, technology, and urban economies.",
      image: "https://images.unsplash.com/photo-1593642634524-b40b5baae6bb?q=80&w=500&auto=format&fit=crop",
      category: "Workplace",
      date: "May 5, 2025",
    },
    {
      title: "Quantum Computing: The Next Frontier in Financial Technology",
      description: "How quantum computing could revolutionize algorithmic trading and risk management.",
      image: "https://images.unsplash.com/photo-1635070041078-e363dbe005cb?q=80&w=500&auto=format&fit=crop",
      category: "Technology",
      date: "May 3, 2025",
    },
  ];

  return (
    <div className="flex flex-col">
      {/* Hero Section */}
      <section className="relative overflow-hidden bg-gradient-to-b from-background to-muted/50 py-12 md:py-20 lg:py-32">
        <div className="absolute inset-0 z-0 opacity-30">
          <svg
            className="h-full w-full"
            viewBox="0 0 1000 1000"
            xmlns="http://www.w3.org/2000/svg"
          >
            <defs>
              <linearGradient id="b" gradientTransform="rotate(45 0.5 0.5)">
                <stop offset="0%" stopColor="hsl(var(--primary))" stopOpacity="0.3" />
                <stop offset="100%" stopColor="hsl(var(--primary))" stopOpacity="0" />
              </linearGradient>
              <pattern id="p" width="100" height="100" patternUnits="userSpaceOnUse">
                <path d="M0 100V0h100" fill="none" stroke="url(#b)" strokeWidth="1" />
              </pattern>
            </defs>
            <rect width="100%" height="100%" fill="url(#p)" />
          </svg>
        </div>

        <div className="container relative z-10">
          <div className="mx-auto max-w-3xl text-center">
            <motion.h1
              className="mb-4 text-3xl font-bold tracking-tight sm:text-4xl md:text-5xl lg:text-6xl"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5 }}
            >
              Trend Analysis
            </motion.h1>
            <motion.p
              className="mb-6 text-base text-muted-foreground sm:text-lg md:text-xl"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.1 }}
            >
              Discover emerging patterns and make data-driven decisions with our comprehensive trend analysis tools.
            </motion.p>
          </div>
        </div>
      </section>

      {/* Market Overview Section */}
      <section className="py-12">
        <div className="container">
          <div className="mb-8">
            <h2 className="text-2xl font-bold sm:text-3xl">Market Overview</h2>
            <p className="text-muted-foreground">Current market trends and key indicators</p>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Market Trend Analysis</CardTitle>
              <CardDescription>
                Year-to-date market performance across major indices
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-[300px] sm:h-[400px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={marketTrendData}>
                    <defs>
                      <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(var(--chart-1))" stopOpacity={0.8} />
                        <stop offset="95%" stopColor="hsl(var(--chart-1))" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <XAxis dataKey="name" />
                    <YAxis />
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "hsl(var(--card))",
                        borderColor: "hsl(var(--border))",
                      }}
                    />
                    <Area
                      type="monotone"
                      dataKey="value"
                      stroke="hsl(var(--chart-1))"
                      fillOpacity={1}
                      fill="url(#colorValue)"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </div>
      </section>

      {/* Trend Categories Section */}
      <section className="py-12 bg-muted/30">
        <div className="container">
          <div className="mb-8">
            <h2 className="text-2xl font-bold sm:text-3xl">Explore Trend Categories</h2>
            <p className="text-muted-foreground">Dive deep into specific areas of market analysis</p>
          </div>

          <motion.div
            className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4"
            variants={container}
            initial="hidden"
            whileInView="show"
            viewport={{ once: true }}
          >
            {trendCategories.map((category, index) => (
              <motion.div key={index} variants={item}>
                <Card className="h-full transition-all duration-300 hover:shadow-md">
                  <CardHeader>
                    <div className={`mb-4 inline-flex rounded-lg p-3 ${category.color}`}>
                      <category.icon className="h-6 w-6" />
                    </div>
                    <CardTitle>{category.title}</CardTitle>
                    <CardDescription>{category.description}</CardDescription>
                  </CardHeader>
                  <CardFooter>
                    <Button variant="outline" className="w-full" asChild>
                      <Link href="#">
                        Explore
                        <ArrowRight className="ml-2 h-4 w-4" />
                      </Link>
                    </Button>
                  </CardFooter>
                </Card>
              </motion.div>
            ))}
          </motion.div>
        </div>
      </section>

      {/* Trend Articles Section */}
      <section className="py-12">
        <div className="container">
          <div className="mb-8">
            <h2 className="text-2xl font-bold sm:text-3xl">Latest Trend Insights</h2>
            <p className="text-muted-foreground">Expert analysis and in-depth articles on market trends</p>
          </div>

          <Tabs defaultValue="all" className="mb-8">
            <TabsList className="flex-wrap">
              <TabsTrigger value="all">All</TabsTrigger>
              <TabsTrigger value="technology">Technology</TabsTrigger>
              <TabsTrigger value="sustainability">Sustainability</TabsTrigger>
              <TabsTrigger value="cryptocurrency">Cryptocurrency</TabsTrigger>
              <TabsTrigger value="economy">Economy</TabsTrigger>
            </TabsList>
          </Tabs>

          <motion.div
            className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3"
            variants={container}
            initial="hidden"
            whileInView="show"
            viewport={{ once: true }}
          >
            {trendArticles.map((article, index) => (
              <motion.div key={index} variants={item}>
                <Card className="overflow-hidden transition-all duration-300 hover:shadow-md">
                  <div className="aspect-video overflow-hidden">
                    <Image
                      src={article.image}
                      alt={article.title}
                      width={500}
                      height={300}
                      className="h-full w-full object-cover transition-transform duration-300 hover:scale-105"
                    />
                  </div>
                  <CardHeader className="pb-2">
                    <div className="flex items-center gap-2">
                      <span className="rounded-full bg-primary/10 px-2 py-1 text-xs font-medium text-primary">
                        {article.category}
                      </span>
                      <span className="text-xs text-muted-foreground">
                        {article.date}
                      </span>
                    </div>
                    <CardTitle className="line-clamp-2">{article.title}</CardTitle>
                    <CardDescription className="line-clamp-2">
                      {article.description}
                    </CardDescription>
                  </CardHeader>
                  <CardFooter>
                    <Button variant="outline" className="w-full" asChild>
                      <Link href="#">
                        Read More
                        <ArrowRight className="ml-2 h-4 w-4" />
                      </Link>
                    </Button>
                  </CardFooter>
                </Card>
              </motion.div>
            ))}
          </motion.div>

          <div className="mt-8 text-center">
            <Button size="lg" asChild>
              <Link href="#">
                View All Articles
                <ArrowRight className="ml-2 h-4 w-4" />
              </Link>
            </Button>
          </div>
        </div>
      </section>

      {/* Custom Analysis Section */}
      <section className="bg-muted/30 py-12">
        <div className="container">
          <div className="mx-auto max-w-3xl text-center">
            <h2 className="mb-4 text-2xl font-bold sm:text-3xl">Create Custom Analysis</h2>
            <p className="mb-8 text-muted-foreground">
              Build your own trend analysis dashboard with our powerful tools
            </p>

            <Card>
              <CardHeader>
                <CardTitle>Custom Trend Dashboard</CardTitle>
                <CardDescription>
                  Select parameters to generate a personalized trend analysis
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid gap-6 md:grid-cols-2">
                  <div>
                    <label className="mb-2 block text-sm font-medium">Time Period</label>
                    <select className="w-full rounded-md border border-input bg-background px-3 py-2">
                      <option>Last 30 Days</option>
                      <option>Last Quarter</option>
                      <option>Year to Date</option>
                      <option>Last 12 Months</option>
                      <option>Custom Range</option>
                    </select>
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium">Market Sector</label>
                    <select className="w-full rounded-md border border-input bg-background px-3 py-2">
                      <option>All Sectors</option>
                      <option>Technology</option>
                      <option>Healthcare</option>
                      <option>Financial</option>
                      <option>Consumer Discretionary</option>
                      <option>Energy</option>
                    </select>
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium">Trend Type</label>
                    <select className="w-full rounded-md border border-input bg-background px-3 py-2">
                      <option>Price Movement</option>
                      <option>Volume Analysis</option>
                      <option>Momentum Indicators</option>
                      <option>Volatility Metrics</option>
                      <option>Correlation Analysis</option>
                    </select>
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium">Visualization</label>
                    <select className="w-full rounded-md border border-input bg-background px-3 py-2">
                      <option>Line Chart</option>
                      <option>Bar Chart</option>
                      <option>Candlestick Chart</option>
                      <option>Heat Map</option>
                      <option>Scatter Plot</option>
                    </select>
                  </div>
                </div>
              </CardContent>
              <CardFooter>
                <Button className="w-full">Generate Analysis</Button>
              </CardFooter>
            </Card>
          </div>
        </div>
      </section>

      {/* Newsletter Section */}
      <section className="py-12">
        <div className="container">
          <div className="rounded-lg border bg-card p-6 shadow-sm sm:p-8">
            <div className="mx-auto max-w-3xl text-center">
              <h2 className="mb-4 text-2xl font-bold">Stay Updated with Trend Insights</h2>
              <p className="mb-6 text-muted-foreground">
                Subscribe to our newsletter to receive weekly trend analysis and market insights
              </p>
              <div className="flex flex-col gap-4 sm:flex-row">
                <input
                  type="email"
                  placeholder="Enter your email"
                  className="flex-1 rounded-md border border-input bg-background px-3 py-2"
                />
                <Button>Subscribe</Button>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}