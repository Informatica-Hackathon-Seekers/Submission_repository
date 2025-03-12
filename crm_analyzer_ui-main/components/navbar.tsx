"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion, AnimatePresence } from "framer-motion";
import { ThemeToggle } from "./theme-toggle";
import { Button } from "./ui/button";
import { BarChart3, TrendingUp, LineChart, Menu, X, User, LogOut, UserCircle, Mail, MailOpen, FileText, Loader2 } from "lucide-react";
import { useState, useEffect } from "react";
import { useSession, signOut } from "next-auth/react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import toast, { Toaster } from "react-hot-toast";
import stockMapping from "@/constants/stockMapping";
import { useUserPreferences } from "@/contexts/userPreferenceContext"; // Import the hook

const topicsOfInterest = [
  "Minerals",
  "Technology",
  "Real Estate",
  "Politics",
  "Healthcare",
  "Energy",
  "Consumer Goods",
  "Financial Services",
  "Telecommunications",
  "Utilities",
  "Electronics",
];

const Navbar = () => {
  const pathname = usePathname();
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const { data: session } = useSession();
  const [isMailOpen, setIsMailOpen] = useState(false);
  const [showNotification, setShowNotification] = useState(false);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [searchValue, setSearchValue] = useState("");
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false); // Loading state for the Save button

  // Use the UserPreferences context
  const { preferences, setPreferences, isLoading: isPreferencesLoading } = useUserPreferences();
  const { topStocks: selectedStocks, topics: selectedTopics } = preferences;

  const navItems = [
    {
      name: "Discover with Us",
      path: "/",
      icon: <TrendingUp className="h-4 w-4 mr-2" />,
      disabled: false,
    },
    {
      name: "Trend Analysis",
      path: "/trend-analysis",
      icon: <BarChart3 className="h-4 w-4 mr-2" />,
      disabled: !session,
    },
    {
      name: "Stock Analysis",
      path: "/stock-analysis",
      icon: <LineChart className="h-4 w-4 mr-2" />,
      disabled: !session,
    },
  ];

  const toggleMenu = () => {
    setIsMenuOpen(!isMenuOpen);
  };

  useEffect(() => {
    if (session) {
      const timer = setTimeout(() => {
        setIsMailOpen(true);
        setShowNotification(true);
      }, 5000);

      return () => clearTimeout(timer);
    }
  }, [session]);

  const handleAddStock = () => {
    const inputValue = searchValue.trim().toLowerCase();

    if (inputValue === "") {
      toast.error("Please enter a stock name.");
      return;
    }

    if (selectedStocks.length >= 5) {
      toast.error("You can only select up to 5 stocks.");
      return;
    }

    // Find the mapped ticker symbol
    const ticker = stockMapping[inputValue];

    if (!ticker) {
      toast.error("Invalid stock name.");
      return;
    }

    // Add the mapped ticker to the selected stocks
    setPreferences({
      ...preferences,
      topStocks: [...selectedStocks, ticker],
    });
    setSearchValue("");
  };

  const handleRemoveStock = (stock: string) => {
    setPreferences({
      ...preferences,
      topStocks: selectedStocks.filter((s) => s !== stock),
    });
  };

  const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setSearchValue(value);

    if (value.trim() === "") {
      setSuggestions([]);
      return;
    }

    // Filter the stockMapping keys based on the input value
    const filteredSuggestions = Object.keys(stockMapping).filter((stock) =>
      stock.toLowerCase().includes(value.toLowerCase())
    );

    setSuggestions(filteredSuggestions);
  };

  const handleTopicClick = (topic: string) => {
    if (selectedTopics.includes(topic)) {
      setPreferences({
        ...preferences,
        topics: selectedTopics.filter((t) => t !== topic),
      });
    } else {
      if (selectedTopics.length >= 3) {
        toast.error("You can only select up to 3 topics.");
        return;
      }
      setPreferences({
        ...preferences,
        topics: [...selectedTopics, topic],
      });
    }
  };

  const handleUpdateStocks = async () => {
    if (!session?.user?.email) {
      toast.error("You must be logged in to update your preferences.");
      return;
    }

    setIsLoading(true); // Start loading

    try {
      const payload = {
        topStocks: selectedStocks,
        topics: selectedTopics,
      };

      console.log("payload : ", payload);
      const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
      const addUserPreferenceEndpoint = process.env.NEXT_PUBLIC_API_ADD_USER_PREFERENCE;

      if (!baseUrl || !addUserPreferenceEndpoint) {
        throw new Error("API configuration is missing.");
      }

      const url = `${baseUrl}${addUserPreferenceEndpoint}?email_id=${encodeURIComponent(
        session.user.email
      )}&preference=${encodeURIComponent(JSON.stringify(payload))}`;
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });

      const data = await response.json();

      console.log("data : ", data);
      if (response.ok) {
        toast.success("Preferences updated successfully!");
        setIsDialogOpen(false);
      } else {
        toast.error(data.message || "Something went wrong.");
      }
    } catch (error) {
      console.error("Error updating preferences:", error);
      toast.error("Something went wrong.");
    } finally {
      setIsLoading(false); // Stop loading
    }
  };

  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-16 items-center justify-between px-4 sm:px-6 lg:px-8">
        <div className="flex items-center gap-4">
          <Link href="/" className="flex items-center space-x-2">
            <motion.div
              initial={{ rotate: -10, scale: 0.9 }}
              animate={{ rotate: 0, scale: 1 }}
              transition={{ duration: 0.5 }}
            >
              <TrendingUp className="h-6 w-6" />
            </motion.div>
            <motion.span
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.1, duration: 0.5 }}
              className="font-bold text-xl"
            >
              Trendlytics
            </motion.span>
          </Link>
        </div>

        <nav className="hidden md:flex mx-auto">
          <ul className="flex space-x-6">
            {navItems.map((item) => {
              const isActive = pathname === item.path;
              return (
                <li key={item.path}>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <div>
                          <Link
                            href={item.disabled ? "#" : item.path}
                            className={item.disabled ? "pointer-events-none" : ""}
                          >
                            <motion.div
                              className={`relative flex items-center px-3 py-2 text-sm font-medium transition-colors ${
                                isActive
                                  ? "text-primary"
                                  : "text-muted-foreground hover:text-primary"
                              }`}
                              whileHover={{ scale: 1.05 }}
                              whileTap={{ scale: 0.95 }}
                            >
                              {item.icon}
                              {item.name}
                              {isActive && (
                                <motion.div
                                  className="absolute bottom-0 left-0 h-0.5 w-full bg-primary"
                                  layoutId="navbar-indicator"
                                  transition={{ type: "spring", stiffness: 350, damping: 30 }}
                                />
                              )}
                            </motion.div>
                          </Link>
                        </div>
                      </TooltipTrigger>
                      {item.disabled && (
                        <TooltipContent>
                          <p>Login to access this feature</p>
                        </TooltipContent>
                      )}
                    </Tooltip>
                  </TooltipProvider>
                </li>
              );
            })}
          </ul>
        </nav>

        <div className="flex items-center gap-4">
          {/* <ThemeToggle /> */}
          {session ? (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" className="rounded-full">
                  <UserCircle className="h-5 w-5" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem>
                  <User className="mr-2 h-4 w-4" />
                  <span>Profile</span>
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => setIsDialogOpen(true)}>
                  <LineChart className="mr-2 h-4 w-4" />
                  <span>Fav Stock</span>
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => signOut()}>
                  <LogOut className="mr-2 h-4 w-4" />
                  <span>Logout</span>
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          ) : (
            <Link href="/auth">
              <Button variant="default" size="sm" className="hidden md:inline-flex">
                Sign In
              </Button>
            </Link>
          )}
          <button
            onClick={toggleMenu}
            className="md:hidden p-2 text-muted-foreground hover:text-primary"
          >
            {isMenuOpen ? <X className="h-6 w-6" /> : <Menu className="h-6 w-6" />}
          </button>
        </div>
      </div>

      {/* Dialog for Updating Favorite Stocks */}
      <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Select Your Preferences</DialogTitle>
            <DialogDescription>
              Select up to 5 stocks and 3 topics to personalize your experience.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            {/* Stock Selection */}
            <div className="flex gap-2 relative">
              <Input
                placeholder="Search for stocks..."
                value={searchValue}
                onChange={handleSearchChange}
              />
              <Button onClick={handleAddStock}>Add</Button>

              {suggestions.length > 0 && (
                <div className="absolute top-full left-0 w-full bg-black border border-gray-200 rounded-lg shadow-lg mt-1 z-10 max-h-60 overflow-y-auto scrollbar">
                  {suggestions.map((suggestion) => (
                    <div
                      key={suggestion}
                      className="p-2 hover:bg-gray-600 cursor-pointer rounded-lg"
                      onClick={() => {
                        setSearchValue(suggestion);
                        setSuggestions([]);
                      }}
                    >
                      {suggestion} ({stockMapping[suggestion]})
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div className="flex flex-wrap gap-2">
              {selectedStocks.map((stock) => (
                <div
                  key={stock}
                  className="flex items-center gap-2 bg-primary/10 px-3 py-1 rounded-full"
                >
                  <span>{stock}</span>
                  <button onClick={() => handleRemoveStock(stock)}>
                    <X className="h-4 w-4" />
                  </button>
                </div>
              ))}
            </div>

            {/* Topic Selection */}
            <div className="space-y-2">
              <h3 className="text-sm font-medium">Topics of Interest</h3>
              <div className="flex flex-wrap gap-2">
                {topicsOfInterest.map((topic) => (
                  <Button
                    key={topic}
                    variant={selectedTopics.includes(topic) ? "default" : "outline"}
                    onClick={() => handleTopicClick(topic)}
                    disabled={selectedTopics.length >= 3 && !selectedTopics.includes(topic)}
                  >
                    {topic}
                  </Button>
                ))}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleUpdateStocks} disabled={isLoading}>
              {isLoading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                "Save"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Mobile Menu */}
      <AnimatePresence>
        {isMenuOpen && (
          <motion.nav
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            transition={{ duration: 0.3 }}
            className="md:hidden bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60"
          >
            <ul className="flex flex-col space-y-2 p-4">
              {navItems.map((item) => {
                const isActive = pathname === item.path;
                return (
                  <li key={item.path}>
                    <Link
                      href={item.disabled ? "#" : item.path}
                      className={item.disabled ? "pointer-events-none" : ""}
                    >
                      <motion.div
                        className={`relative flex items-center px-3 py-2 text-sm font-medium transition-colors ${
                          isActive
                            ? "text-primary"
                            : "text-muted-foreground hover:text-primary"
                        }`}
                        whileHover={{ scale: 1.05 }}
                        whileTap={{ scale: 0.95 }}
                      >
                        {item.icon}
                        {item.name}
                        {isActive && (
                          <motion.div
                            className="absolute bottom-0 left-0 h-0.5 w-full bg-primary"
                            layoutId="navbar-indicator"
                            transition={{ type: "spring", stiffness: 350, damping: 30 }}
                          />
                        )}
                      </motion.div>
                    </Link>
                  </li>
                );
              })}
              <li>
                <Link href="/auth">
                  <Button variant="default" size="sm" className="w-full">
                    Sign In
                  </Button>
                </Link>
              </li>
            </ul>
          </motion.nav>
        )}
      </AnimatePresence>

      {/* Toaster for notifications */}
      <Toaster />
    </header>
  );
};

export default Navbar;