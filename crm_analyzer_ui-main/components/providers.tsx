// src/components/providers.tsx
"use client"; // Mark this component as a Client Component

import { SessionProvider } from "next-auth/react"; // Import SessionProvider
import { ThemeProvider } from "@/components/theme-provider"; // Import ThemeProvider

export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <SessionProvider>
      <ThemeProvider
        attribute="class"
        defaultTheme="system"
        enableSystem
        disableTransitionOnChange
      >
        {children}
      </ThemeProvider>
    </SessionProvider>
  );
}