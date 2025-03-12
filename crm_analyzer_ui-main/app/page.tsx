"use client";

import { motion } from "framer-motion";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Upload, TrendingUp, BarChart, LineChart, FileUp } from "lucide-react";
import { useEffect, useState } from "react";
import { useUserPreferences } from "@/contexts/userPreferenceContext";

// Define types for the API response
type Article = {
  title: string;
  summary: string;
  link: string;
  topic: string;
};

type Source = {
  articles: Article[];
  source: string;
};

type ApiResponse = {
  message: Source[];
};

// Define types for the trend card
type TrendCard = {
  title: string;
  description: string;
  icon: JSX.Element;
  image: string;
  link: string;
  topic: string; // Added topic to TrendCard
};

// Animation variants
const fadeIn = {
  hidden: { opacity: 0, y: 20 },
  visible: (i: number) => ({
    opacity: 1,
    y: 0,
    transition: {
      delay: i * 0.1,
      duration: 0.5,
    },
  }),
};

// Icons and images for each topic
const topicIcons: Record<string, JSX.Element> = {
  "Consumer Goods": <TrendingUp className="h-8 w-8 text-primary" />,
  "Technology": <BarChart className="h-8 w-8 text-primary" />,
  "Financial Services": <LineChart className="h-8 w-8 text-primary" />,
  "Politics": <TrendingUp className="h-8 w-8 text-primary" />,
  "Transportation": <BarChart className="h-8 w-8 text-primary" />,
  "Healthcare": <LineChart className="h-8 w-8 text-primary" />,
  "Telecommunications": <TrendingUp className="h-8 w-8 text-primary" />,
  "Electronics": <BarChart className="h-8 w-8 text-primary" />,
  "Utilities": <LineChart className="h-8 w-8 text-primary" />,
  "default": <TrendingUp className="h-8 w-8 text-primary" />,
};

const topicImages: Record<string, string> = {
  "Consumer Goods": "https://images.unsplash.com/photo-1607082348824-0a96f2a4b9da?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Shopping cart with products
  "Technology": "https://images.unsplash.com/photo-1518770660439-4636190af475?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Circuit boards and tech
  "Financial Services": "https://images.unsplash.com/photo-1454165804606-c3d57bc86b40?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Financial charts and graphs
  "Politics": "https://images.unsplash.com/photo-1507676184212-d03ab07a01bf?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Government buildings or flags
  "Transportation": "https://images.unsplash.com/photo-1502877338535-766e1452684a?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Cars on a highway
  "Healthcare": "https://images.unsplash.com/photo-1576091160550-2173dba999ef?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Hospital or medical equipment
  "Telecommunications": "https://images.unsplash.com/photo-1519501025264-65ba15a82390?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Satellite or communication towers
  "Electronics": "https://images.unsplash.com/photo-1518770660439-4636190af475?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Electronics and gadgets
  "Utilities": "https://images.unsplash.com/photo-1509391366360-2e959784a276?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Power lines or energy sources
  "default": "https://images.unsplash.com/photo-1551288049-bebda4e38f71?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80", // Generic data visualization
};

// List of all topics including "All"
const allTopics = [
  "All",
  "Consumer Goods",
  "Technology",
  "Financial Services",
  "Politics",
  "Transportation",
  "Healthcare",
  "Electronics",
  "Utilities",
  "Telecommunications",
];

const tiers = [
  {
    name: "Basic",
    price: "Free",
    description: "Essential features for casual users",
    features: [
      "Basic trend analysis",
      "Limited stock data",
      "Daily market updates",
      "1 file upload per day",
    ],
    cta: "Get Started",
    popular: false,
  },
  {
    name: "Pro",
    price: "$29/month",
    description: "Advanced features for serious investors",
    features: [
      "Advanced trend analysis",
      "Real-time stock data",
      "Personalized alerts",
      "Unlimited file uploads",
      "AI-powered insights",
    ],
    cta: "Upgrade to Pro",
    popular: true,
  },
  {
    name: "Enterprise",
    price: "$99/month",
    description: "Complete solution for professional traders",
    features: [
      "All Pro features",
      "API access",
      "Custom reports",
      "Dedicated support",
      "Team collaboration",
      "Advanced analytics",
    ],
    cta: "Contact Sales",
    popular: false,
  },
];

export default function Home() {
  const [apiData, setApiData] = useState<TrendCard[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [visibleCards, setVisibleCards] = useState(6); // Initially show 6 cards (2 rows)
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const { preferences } = useUserPreferences(); // Access user preferences
  const [selectedTopic, setSelectedTopic] = useState<string>("All"); // Track selected topic

  const userPreferredTopics = preferences.topics || [];

  // Combine "All" with user's preferred topics
  const allTopics = ["All", ...userPreferredTopics];

  const fetchData = async (topic: string = "All") => {
    setLoading(true);
    setApiData(null); // Clear existing cards
    try {
      const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
      const endpoint = process.env.NEXT_PUBLIC_API_GET_LATEST_NEWS_SNIPPETS;
  
      if (!baseUrl || !endpoint) {
        throw new Error("API configuration is missing.");
      }
  
      const url =
        topic === "All"
          ? `${baseUrl}${endpoint}`
          : `${baseUrl}${endpoint}/?topic=${topic}`;
  
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error("Network response was not ok");
      }
  
      const data = await response.json();
      console.log("API Response:", data);
  
      // Use a Set to track unique titles
      const uniqueTitles = new Set<string>();
  
      // Transform API data into trendCards format, filtering out duplicates
      const transformedData = data.message.flatMap((source: Source) =>
        source.articles
          .filter((article: Article) => {
            if (uniqueTitles.has(article.title)) {
              return false; // Skip if title is already in the Set
            }
            uniqueTitles.add(article.title); // Add title to the Set
            return true;
          })
          .map((article: Article) => ({
            title: article.title,
            description: article.summary,
            icon: topicIcons[article.topic] || topicIcons["default"],
            image: topicImages[article.topic] || topicImages["default"],
            link: article.link,
            topic: article.topic,
          }))
      );
  
      setApiData(transformedData);
    } catch (error) {
      console.error("Error fetching data:", error);
      setError(error instanceof Error ? error.message : "An unknown error occurred");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData(selectedTopic);
  }, [selectedTopic]);

  const handleLoadMore = () => {
    setIsLoadingMore(true);
    setTimeout(() => {
      setVisibleCards((prev) => prev + 6); // Load 6 more cards (2 rows)
      setIsLoadingMore(false);
    }, 3000); // Simulate a 3-second loading delay
  };

  const handleTopicClick = (topic: string) => {
    setSelectedTopic(topic);
    setVisibleCards(6); // Reset visible cards when topic changes
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center h-screen">
        <div className="flex items-center justify-center">
          <div className="animate-ping h-4 w-4 rounded-full bg-primary absolute"></div>
          <div className="animate-ping h-6 w-6 rounded-full bg-primary absolute delay-200"></div>
          <div className="animate-ping h-8 w-8 rounded-full bg-primary absolute delay-400"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return <div>Error: {error}</div>;
  }

  return (
    <div className="flex flex-col items-center">
      {/* Hero Section */}
      <section className="w-full py-12 md:py-24 lg:py-32 bg-gradient-to-br from-background via-background to-muted dark:from-background dark:via-background dark:to-accent">
        <div className="container px-4 md:px-6">
          <div className="grid gap-6 lg:grid-cols-[1fr_400px] lg:gap-12 xl:grid-cols-[1fr_600px]">
            <motion.div
              className="flex flex-col justify-center space-y-4"
              initial="hidden"
              animate="visible"
              variants={fadeIn}
              custom={0}
            >
              <div className="space-y-2">
                <motion.h1
                  className="text-3xl font-bold tracking-tighter sm:text-5xl xl:text-6xl/none bg-clip-text text-transparent bg-gradient-to-r from-primary to-indigo-500 dark:from-primary dark:to-indigo-400"
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.2, duration: 0.5 }}
                >
                  Discover with Us
                </motion.h1>
                <motion.p
                  className="max-w-[600px] text-muted-foreground md:text-xl"
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.3, duration: 0.5 }}
                >
                  Your gateway to the latest trends, stock analysis, and more. Unlock insights, elevate decisions.
                </motion.p>
              </div>
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.4, duration: 0.5 }}
              >
                <Button size="lg" className="bg-primary hover:bg-primary/90">
                  Get Started
                </Button>
              </motion.div>
            </motion.div>
            <motion.div
              className="flex items-center justify-center"
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: 0.5, duration: 0.5 }}
            >
              <div className="relative h-[350px] w-full overflow-hidden rounded-xl bg-muted">
                <div className="absolute inset-0 bg-gradient-to-br from-violet-500/20 via-transparent to-blue-500/20 dark:from-violet-400/10 dark:to-blue-400/10" />
                <img
                  src="https://images.unsplash.com/photo-1551288049-bebda4e38f71?ixlib=rb-4.0.3&auto=format&fit=crop&w=1200&q=80"
                  alt="Data visualization"
                  className="h-full w-full object-cover"
                />
              </div>
            </motion.div>
          </div>
        </div>
      </section>

      {/* Trend Cards Section */}
      <section className="w-full py-12 md:py-24 bg-background">
        <div className="container px-4 md:px-6">
          <div className="flex flex-col items-center justify-center space-y-4 text-center">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5 }}
              viewport={{ once: true }}
            >
              <h2 className="text-3xl font-bold tracking-tighter sm:text-4xl md:text-5xl">Latest Trends</h2>
              <p className="mx-auto mt-4 max-w-[700px] text-muted-foreground md:text-xl">
                Stay updated with the most recent market trends and insights
              </p>
            </motion.div>
          </div>

          {/* Topics Section */}
          <div className="flex flex-wrap justify-center gap-2 mt-6">
            {allTopics.map((topic, index) => (
              <Button
                key={index}
                variant={selectedTopic === topic ? "default" : "outline"}
                className="rounded-full"
                onClick={() => handleTopicClick(topic)}
              >
                {topic}
              </Button>
            ))}
          </div>

          {/* Cards Grid */}
          <div className="mx-auto grid max-w-5xl grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-3 mt-12">
            {apiData?.slice(0, visibleCards).map((card, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1, duration: 0.5 }}
                viewport={{ once: true }}
                whileHover={{ y: -5, transition: { duration: 0.2 } }}
              >
                <Card className="relative overflow-hidden">
                  {/* Topic Tag */}
                  <div className="absolute right-0 top-0">
                    <div className="bg-primary px-3 py-1 text-xs font-medium text-primary-foreground rounded-bl-lg">
                      {card.topic}
                    </div>
                  </div>

                  {/* Card Image */}
                  <div className="h-48 overflow-hidden">
                    <img
                      src={card.image}
                      alt={card.title}
                      className="h-full w-full object-cover transition-transform duration-300 hover:scale-105"
                    />
                  </div>

                  {/* Card Content */}
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <CardTitle>{card.title}</CardTitle>
                    </div>
                    <CardDescription>{card.description}</CardDescription>
                  </CardHeader>
                  <CardFooter>
                    <Button
                      variant="outline"
                      className="w-full"
                      onClick={() => {
                        const fullLink = card.link.startsWith("http") ? card.link : `https://${card.link}`;
                        window.open(fullLink, "_blank");
                      }}
                    >
                      Learn More
                    </Button>
                  </CardFooter>
                </Card>
              </motion.div>
            ))}
          </div>

          {/* Load More Button */}
          {apiData && visibleCards < apiData.length && (
            <div className="flex justify-center mt-8">
              <Button
                onClick={handleLoadMore}
                disabled={isLoadingMore}
                className="relative"
              >
                {isLoadingMore ? (
                  <div className="flex items-center justify-center">
                    <div className="animate-ping h-4 w-4 rounded-full bg-primary absolute"></div>
                    <div className="animate-ping h-6 w-6 rounded-full bg-primary absolute delay-200"></div>
                    <div className="animate-ping h-8 w-8 rounded-full bg-primary absolute delay-400"></div>
                  </div>
                ) : (
                  "Load More"
                )}
              </Button>
            </div>
          )}
        </div>
      </section>

      {/* File Upload Section */}
      {/*<section className="w-full py-12 md:py-24 bg-muted/50">
        <div className="container px-4 md:px-6">
          <motion.div
            className="mx-auto flex max-w-5xl flex-col items-center justify-center space-y-4 text-center"
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            viewport={{ once: true }}
          >
            <div className="space-y-2">
              <h2 className="text-3xl font-bold tracking-tighter sm:text-4xl md:text-5xl">
                Extract Valuable Insights
              </h2>
              <p className="mx-auto mt-4 max-w-[700px] text-muted-foreground md:text-xl">
                Upload your files and let our AI analyze them for valuable market insights
              </p>
            </div>
          </motion.div>
          <motion.div
            className="mx-auto mt-12 max-w-3xl rounded-xl border bg-card p-8 shadow-lg"
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2, duration: 0.5 }}
            viewport={{ once: true }}
          >
            <div className="flex flex-col items-center space-y-6">
              <div className="flex h-20 w-20 items-center justify-center rounded-full bg-primary/10">
                <Upload className="h-10 w-10 text-primary" />
              </div>
              <div className="space-y-2 text-center">
                <h3 className="text-xl font-bold">Upload Files</h3>
                <p className="text-muted-foreground">
                  Drag and drop your files here or click to browse
                </p>
              </div>
              <div className="grid w-full gap-4">
                <Button className="w-full">
                  <FileUp className="mr-2 h-4 w-4" />
                  Select Files
                </Button>
                <Button variant="outline" className="w-full">
                  Extract Information
                </Button>
              </div>
            </div>
          </motion.div>
        </div>
      </section>*/}

      {/* Tier Information Section */}
      <section className="w-full py-12 md:py-24 bg-muted/30">
        <div className="container px-4 md:px-6">
          <motion.div
            className="mx-auto flex max-w-5xl flex-col items-center justify-center space-y-4 text-center"
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            viewport={{ once: true }}
          >
            <div className="space-y-2">
              <h2 className="text-3xl font-bold tracking-tighter sm:text-4xl md:text-5xl">
                Choose Your Plan
              </h2>
              <p className="mx-auto mt-4 max-w-[700px] text-muted-foreground md:text-xl">
                Select the perfect plan that suits your needs and budget
              </p>
            </div>
          </motion.div>
          <div className="mx-auto mt-12 grid max-w-5xl grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-3">
            {tiers.map((tier, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1, duration: 0.5 }}
                viewport={{ once: true }}
                whileHover={{ y: -5, transition: { duration: 0.2 } }}
              >
                <Card className={`relative overflow-hidden ${tier.popular ? 'border-primary shadow-md' : ''}`}>
                  {tier.popular && (
                    <div className="absolute right-0 top-0">
                      <div className="bg-primary px-3 py-1 text-xs font-medium text-primary-foreground">
                        Popular
                      </div>
                    </div>
                  )}
                  <CardHeader>
                    <CardTitle>{tier.name}</CardTitle>
                    <div className="mt-2">
                      <span className="text-3xl font-bold">{tier.price}</span>
                    </div>
                    <CardDescription>{tier.description}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <ul className="space-y-2">
                      {tier.features.map((feature, i) => (
                        <li key={i} className="flex items-center">
                          <svg
                            xmlns="http://www.w3.org/2000/svg"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="2"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            className="mr-2 h-4 w-4 text-primary"
                          >
                            <path d="M20 6 9 17l-5-5" />
                          </svg>
                          <span className="text-sm">{feature}</span>
                        </li>
                      ))}
                    </ul>
                  </CardContent>
                  <CardFooter>
                    <Button
                      variant={tier.popular ? "default" : "outline"}
                      className="w-full"
                    >
                      {tier.cta}
                    </Button>
                  </CardFooter>
                </Card>
              </motion.div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}