"use client";

import { useState, useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { MessageSquare, X, Send, Minimize2, Maximize2 } from "lucide-react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { ScrollArea } from "./ui/scroll-area";
import fuzzy from "fuzzy";
import stockMapping from "@/constants/stockMapping";

type Message = {
  id: number;
  text: string;
  isUser: boolean;
};

const Chatbot = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [isMinimized, setIsMinimized] = useState(false);
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<Message[]>([
    {
      id: 1,
      text: "Hi there! How can I help you today?",
      isUser: false,
    },
  ]);
  const [currentNews, setCurrentNews] = useState(null);
  const [history, setHistory] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [loadingText, setLoadingText] = useState("");
  const [lastStockSymbol, setLastStockSymbol] = useState<string | null>(null);
  const waitMessages = ["Processing your query...", "Almost there...", "Finalizing..."];
  let textIndex = 0;

  const scrollAreaRef = useRef<HTMLDivElement>(null);

  const extractStockName = (userInput: string): string | null => {
    const cleanedInput = userInput.toLowerCase().replace(/[^a-zA-Z\s]/g, "");
    const words = cleanedInput.split(/\s+/);

    let bestMatch: fuzzy.FilterResult<string> | null = null;

    for (const word of words) {
      if (stockMapping[word]) {
        return word;
      }

      const matches = fuzzy.filter(word, Object.keys(stockMapping));
      if (matches.length > 0) {
        const topMatch = matches[0];
        if (!bestMatch || topMatch.score > bestMatch.score) {
          bestMatch = topMatch;
        }
      }
    }

    if (bestMatch && bestMatch.score > 50) {
      return bestMatch.string;
    }

    for (const stockName of Object.keys(stockMapping)) {
      if (cleanedInput.includes(stockName)) {
        return stockName;
      }
    }

    return null;
  };

  const handleSendMessage = async () => {
    if (input.trim() === "") return;
  
    const newUserMessage: Message = {
      id: messages.length + 1,
      text: input,
      isUser: true,
    };
    setMessages((prev) => [...prev, newUserMessage]);
    setInput("");
    setIsLoading(true);
    setLoadingText("Analysing data...");
  
    const stockName = extractStockName(input);
    const stockSymbol = stockName ? stockMapping[stockName] : lastStockSymbol;
  
    if (!stockSymbol) {
      setIsLoading(false);
      const aiResponse: Message = {
        id: messages.length + 2,
        text: "Sorry, I couldn't find that stock. Please try again.",
        isUser: false,
      };
      setMessages((prev) => [...prev, aiResponse]);
      return;
    }
  
    // Reset current_news if stock symbol changes
    if (lastStockSymbol !== stockSymbol) {
      console.log("currentNews : ", currentNews);
      setCurrentNews(null);
    }
    setLastStockSymbol(stockSymbol);
  
    console.log("stockSymbol : ", stockSymbol);
    console.log("input : ", input);
    console.log("history : ", history);
  
    try {
      // Prepare the request body
      const requestBody = history ? history : [];
      // Get environment variables
      const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
      const chatEndpoint = process.env.NEXT_PUBLIC_API_CHAT_ENDPOINT;

      if (!baseUrl || !chatEndpoint) {
        throw new Error("API configuration is missing.");
      }
    
      // Construct the URL dynamically
      const url = `${baseUrl}${chatEndpoint}?message=${encodeURIComponent(
        input
      )}&stock_name=${stockSymbol}`;
  
       const response = await fetch(url,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(requestBody), // Send history directly as an array
        }
      );
  
      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
  
      const data = await response.json();
  
      console.log("data : ", data);
  
      setCurrentNews(data.current_news);
      setHistory(data.history);
  
      const aiResponse: Message = {
        id: messages.length + 2,
        text: data.message,
        isUser: false,
      };
      setMessages((prev) => [...prev, aiResponse]);
    } catch (error) {
      console.error("Error fetching data:", error);
      const aiResponse: Message = {
        id: messages.length + 2,
        text: "Failed to fetch data. Please try again.",
        isUser: false,
      };
      setMessages((prev) => [...prev, aiResponse]);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    if (isLoading) {
      const textTimer = setInterval(() => {
        setLoadingText(waitMessages[textIndex % waitMessages.length]);
        textIndex++;
      }, 4000); // Smooth transition every 4 seconds

      return () => clearInterval(textTimer);
    }
  }, [isLoading]);

  useEffect(() => {
    if (scrollAreaRef.current) {
      scrollAreaRef.current.scrollTo({
        top: scrollAreaRef.current.scrollHeight,
        behavior: "smooth",
      });
    }
  }, [messages]);

  const toggleChatbot = () => {
    setIsOpen(!isOpen);
    setIsMinimized(false);
  };

  const toggleMinimize = () => {
    setIsMinimized(!isMinimized);
  };

  return (
    <div className="fixed bottom-4 right-4 z-50">
      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ opacity: 0, y: 50, scale: 0.9 }}
            animate={
              isMinimized
                ? { opacity: 1, y: 0, scale: 0.9, height: "auto" }
                : { opacity: 1, y: 0, scale: 1, height: "auto" }
            }
            exit={{ opacity: 0, y: 50, scale: 0.9 }}
            transition={{ type: "spring", stiffness: 300, damping: 25 }}
            className="mb-4 w-80 rounded-lg border bg-card shadow-lg md:w-96"
          >
            <div className="flex items-center justify-between border-b p-3">
              <div className="flex items-center space-x-2">
                <MessageSquare className="h-5 w-5" />
                <h3 className="font-medium">Trendlytics Assistant</h3>
              </div>
              <div className="flex items-center space-x-1">
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7"
                  onClick={toggleMinimize}
                >
                  {isMinimized ? (
                    <Maximize2 className="h-4 w-4" />
                  ) : (
                    <Minimize2 className="h-4 w-4" />
                  )}
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7"
                  onClick={toggleChatbot}
                >
                  <X className="h-4 w-4" />
                </Button>
              </div>
            </div>

            <AnimatePresence>
              {!isMinimized && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <ScrollArea ref={scrollAreaRef} className="h-80 p-4">
                    <div className="space-y-4">
                      {messages.map((message) => (
                        <motion.div
                          key={message.id}
                          initial={{ opacity: 0, y: 10 }}
                          animate={{ opacity: 1, y: 0 }}
                          transition={{ duration: 0.3 }}
                          className={`flex ${
                            message.isUser ? "justify-end" : "justify-start"
                          }`}
                        >
                          <div
                            className={`max-w-[80%] rounded-lg px-3 py-2 ${
                              message.isUser
                                ? "bg-primary text-primary-foreground"
                                : "bg-muted"
                            }`}
                          >
                            <p className="text-sm">{message.text}</p>
                          </div>
                        </motion.div>
                      ))}
                      {isLoading && (
                        <motion.div
                          initial={{ opacity: 0, filter: "blur(8px)" }}
                          animate={{
                            opacity: 1,
                            filter: "blur(0px)",
                            transition: { duration: 1, ease: "easeOut" }
                          }}
                          exit={{
                            opacity: 0,
                            filter: "blur(8px)",
                            transition: { duration: 1, ease: "easeIn" }
                          }}
                          className="flex justify-start"
                        >
                          <div className="max-w-[80%] rounded-lg bg-muted px-3 py-1.5 text-sm border border-muted-foreground/20">
                            <AnimatePresence mode="wait">
                              <motion.span
                                key={loadingText}
                                initial={{ opacity: 0, filter: "blur(8px)" }}
                                animate={{
                                  opacity: 1,
                                  filter: "blur(0px)",
                                  transition: { duration: 2, ease: "easeOut" }
                                }}
                                exit={{
                                  opacity: 0,
                                  filter: "blur(8px)",
                                  transition: { duration: 2, ease: "easeIn" }
                                }}
                                className="inline-block"
                              >
                                {loadingText}
                              </motion.span>
                            </AnimatePresence>
                          </div>
                        </motion.div>
                      )}
                    </div>
                  </ScrollArea>

                  <div className="border-t p-3">
                    <form
                      onSubmit={(e) => {
                        e.preventDefault();
                        handleSendMessage();
                      }}
                      className="flex space-x-2"
                    >
                      <Input
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        placeholder="Ask me anything..."
                        className="flex-1"
                      />
                      <Button type="submit" size="icon" className="shrink-0">
                        <Send className="h-4 w-4" />
                      </Button>
                    </form>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        )}
      </AnimatePresence>

      <motion.button
        onClick={toggleChatbot}
        className="flex h-12 w-12 items-center justify-center rounded-full bg-primary text-primary-foreground shadow-lg"
        whileHover={{ scale: 1.1 }}
        whileTap={{ scale: 0.9 }}
      >
        <MessageSquare className="h-6 w-6" />
        </motion.button>
    </div>
  );
};

export default Chatbot;