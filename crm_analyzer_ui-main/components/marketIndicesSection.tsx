"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { TrendingUp, TrendingDown, RefreshCw, ChevronLeft, ChevronRight } from "lucide-react";

// Mock data for market indices
const marketIndices = [
  { name: "Sensex", value: "59,832.97", change: "+0.7%", up: true },
  { name: "Nifty", value: "17,853.20", change: "+0.8%", up: true },
  { name: "Bank Nifty", value: "41,559.40", change: "-0.3%", up: false },
  { name: "Nifty IT", value: "28,690.65", change: "+1.2%", up: true },
];

const MarketIndicesSection = () => {
  const [lastUpdated, setLastUpdated] = useState(new Date());
  const [activeIndex, setActiveIndex] = useState(0);
  const [isMobile, setIsMobile] = useState(false);

  // Handle window resize to detect mobile view
  useEffect(() => {
    const handleResize = () => {
      setIsMobile(window.innerWidth <= 768);
    };
    handleResize(); // Initial check
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  // Auto-scroll for mobile view
  useEffect(() => {
    if (isMobile) {
      const interval = setInterval(() => {
        setActiveIndex((prev) => (prev + 1) % marketIndices.length);
      }, 3000); // Change index every 3 seconds
      return () => clearInterval(interval);
    }
  }, [isMobile]);

  const handleReload = () => {
    // Simulate a reload action
    setLastUpdated(new Date());
  };

  const handlePrev = () => {
    setActiveIndex((prev) => (prev === 0 ? marketIndices.length - 1 : prev - 1));
  };

  const handleNext = () => {
    setActiveIndex((prev) => (prev + 1) % marketIndices.length);
  };

  return (
    <section className="w-full border-b bg-gradient-to-r from-muted/20 to-muted/10 py-4">
      <div className="container mx-auto px-4 sm:px-6 lg:px-8">
        {/* Desktop View */}
        <div className="hidden md:flex items-center justify-between gap-3 py-2 text-sm">
          {marketIndices.map((index, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.1, duration: 0.3 }}
              className="flex items-center space-x-2"
            >
              <span className="font-medium">{index.name}</span>
              <span>{index.value}</span>
              <span
                className={`flex items-center ${
                  index.up ? "text-green-500" : "text-red-500"
                }`}
              >
                {index.up ? (
                  <TrendingUp className="mr-1 h-3 w-3" />
                ) : (
                  <TrendingDown className="mr-1 h-3 w-3" />
                )}
                {index.change}
              </span>
            </motion.div>
          ))}

          {/* Last Updated and Reload Button */}
          <div className="flex items-center space-x-2 text-sm text-muted-foreground">
            <span>Last updated at: {lastUpdated.toLocaleTimeString()}</span>
            <motion.button
              onClick={handleReload}
              whileHover={{ rotate: 360 }}
              whileTap={{ scale: 0.9 }}
              className="p-1 hover:text-foreground"
            >
              <RefreshCw className="h-4 w-4" />
            </motion.button>
          </div>
        </div>

        {/* Mobile View */}
        <div className="md:hidden relative flex items-center justify-center">
          {/* Left Arrow */}
          <button
            onClick={handlePrev}
            className="absolute left-0 p-2 hover:text-primary"
          >
            <ChevronLeft className="h-5 w-5" />
          </button>

          {/* Active Index */}
          <motion.div
            key={activeIndex}
            initial={{ opacity: 0, x: -10 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: 10 }}
            transition={{ duration: 0.3 }}
            className="flex items-center space-x-2"
          >
            <span className="font-medium">{marketIndices[activeIndex].name}</span>
            <span>{marketIndices[activeIndex].value}</span>
            <span
              className={`flex items-center ${
                marketIndices[activeIndex].up ? "text-green-500" : "text-red-500"
              }`}
            >
              {marketIndices[activeIndex].up ? (
                <TrendingUp className="mr-1 h-3 w-3" />
              ) : (
                <TrendingDown className="mr-1 h-3 w-3" />
              )}
              {marketIndices[activeIndex].change}
            </span>
          </motion.div>

          {/* Right Arrow */}
          <button
            onClick={handleNext}
            className="absolute right-0 p-2 hover:text-primary"
          >
            <ChevronRight className="h-5 w-5" />
          </button>
        </div>

        {/* Last Updated and Reload Button for Mobile */}
        <div className="md:hidden mt-4 flex items-center justify-center space-x-2 text-sm text-muted-foreground">
          <span>Last updated at: {lastUpdated.toLocaleTimeString()}</span>
          <motion.button
            onClick={handleReload}
            whileHover={{ rotate: 360 }}
            whileTap={{ scale: 0.9 }}
            className="p-1 hover:text-foreground"
          >
            <RefreshCw className="h-4 w-4" />
          </motion.button>
        </div>
      </div>
    </section>
  );
};

export default MarketIndicesSection;