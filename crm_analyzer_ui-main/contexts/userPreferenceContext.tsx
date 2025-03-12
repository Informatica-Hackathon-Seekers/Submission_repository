// contexts/UserPreferencesContext.tsx
"use client";

import { createContext, useContext, useState, useEffect, ReactNode } from "react";
import { useSession } from "next-auth/react";
import toast from "react-hot-toast";

type UserPreferences = {
  topStocks: string[];
  topics: string[];
};

type UserPreferencesContextType = {
  preferences: UserPreferences;
  setPreferences: (preferences: UserPreferences) => void;
  isLoading: boolean;
};

const UserPreferencesContext = createContext<UserPreferencesContextType | null>(null);

export const UserPreferencesProvider = ({ children }: { children: ReactNode }) => {
  const { data: session } = useSession();
  const [preferences, setPreferences] = useState<UserPreferences>({
    topStocks: [],
    topics: [],
  });
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (session?.user?.email) {
      const fetchUserPreferences = async () => {
        try {
          const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
          const getUserPreferenceEndpoint = process.env.NEXT_PUBLIC_API_GET_USER_PREFERENCE;

          if (!baseUrl || !getUserPreferenceEndpoint) {
            throw new Error("API configuration is missing.");
          }

          const url = `${baseUrl}${getUserPreferenceEndpoint}?email_id=${encodeURIComponent(
            session.user.email
          )}`;
          const response = await fetch(url);
          const data = await response.json();

          console.log("data inside the context : ",data);

          if (data.message === "User preference not found") {
            toast("Hi, user! You can set your stock preferences to get updates via email.", {
              duration: 6000,
            });
          } else if (data.message && typeof data.message.preference === "string") {
            const parsedPreferences = JSON.parse(data.message.preference);
            setPreferences({
              topStocks: parsedPreferences.topStocks || [],
              topics: parsedPreferences.topics || [],
            });
          } else {
            toast.error("Invalid preference data received.");
          }
        } catch (error) {
          console.error("Error fetching preferences:", error);
          toast.error("Something went wrong while fetching preferences.");
        } finally {
          setIsLoading(false);
        }
      };

      fetchUserPreferences();
    } else {
      setIsLoading(false);
    }
  }, [session?.user?.email]);

  return (
    <UserPreferencesContext.Provider value={{ preferences, setPreferences, isLoading }}>
      {children}
    </UserPreferencesContext.Provider>
  );
};

export const useUserPreferences = () => {
  const context = useContext(UserPreferencesContext);
  if (!context) {
    throw new Error("useUserPreferences must be used within a UserPreferencesProvider");
  }
  return context;
};