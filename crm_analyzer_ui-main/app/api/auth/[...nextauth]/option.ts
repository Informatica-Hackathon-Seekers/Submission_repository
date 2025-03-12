import NextAuth, { DefaultSession, User, NextAuthOptions, Session } from "next-auth";
import { JWT } from "next-auth/jwt";
import { AdapterUser } from "next-auth/adapters";
import CredentialsProvider from "next-auth/providers/credentials";
import GoogleProvider from "next-auth/providers/google";
import { dbConnect } from "@/lib/dbConnect";
import UserModel from "@/model/User";
import bcrypt from "bcryptjs";

// Validate environment variables at initialization
const googleClientId = process.env.GOOGLE_CLIENT_ID;
const googleClientSecret = process.env.GOOGLE_CLIENT_SECRET;

if (!googleClientId || !googleClientSecret) {
  throw new Error(
    "Google OAuth environment variables are not configured properly"
  );
}

// Extend the `User` type to include the `role` property
declare module "next-auth" {
  interface User {
    role?: string;
  }

  interface Session extends DefaultSession {
    user: {
      id: string;
      name: string;
      email: string;
      role: string;
      image?: string;
    } & DefaultSession["user"];
  }
}

// Extend the JWT type to include the `role` field
declare module "next-auth/jwt" {
  interface JWT {
    id: string;
    name: string;
    email: string;
    role: string;
    image?: string;
  }
}

export const authOptions: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        await dbConnect();

        // Find the user by email
        const user = await UserModel.findOne({ email: credentials?.email });
        if (!user) {
          throw new Error("No user found with this email address.");
        }

        // Compare the provided password with the hashed password
        const isValid = await bcrypt.compare(
          credentials?.password || "",
          user.password
        );
        if (!isValid) {
          throw new Error("Incorrect password.");
        }

        // Return the user object
        return {
          id: user._id.toString(),
          name: user.name,
          email: user.email,
          role: user.role,
        };
      },
    }),
    GoogleProvider({
      clientId: googleClientId,
      clientSecret: googleClientSecret,
      profile(profile) {
        return {
          id: profile.sub,
          name: profile.name,
          email: profile.email,
          image: profile.picture,
          role: "user", // Default role for Google sign-in
          provider: "google",
        };
      },
    }),
  ],
  callbacks: {
    async signIn({ user, account }: { user: any; account: any }) {
      await dbConnect();

      console.log("User from Google:", user); // Log the user object from Google
      console.log("Account provider:", account?.provider); // Log the provider

      if (account?.provider === "google") {
        let existingUser = await UserModel.findOne({ email: user.email });

        console.log("Existing user:", existingUser); // Log the existing user

        if (!existingUser) {
          // If the user does not exist, create a new one
          console.log("Creating new user...");
          const newUser = await UserModel.create({
            name: user.name || "Unknown",
            email: user.email,
            image: user.image || "",
            role: "user",
            userType: "personal",
            tier: "Starter",
            topStocks: [],
            isVerified: true,
            provider: "google",
            username: user.name.split("@")[0], // Assign a username based on email
          });          

          console.log("New user created:", existingUser); // Log the newly created user
        } else {
          // Update the existing user's role if necessary
          console.log("Updating existing user...");
          existingUser.role = existingUser.role || "user";
          await existingUser.save();

          console.log("Existing user updated:", existingUser); // Log the updated user
        }
      }

      return true;
    },
    async jwt({ token, user }: { token: any; user: any }) {
      if (user) {
        token.id = user.id;
        token.name = user.name;
        token.email = user.email;
        token.role = user.role;
        token.image = user.image;
      }
      return token;
    },
    async session({ session, token }: { session: any; token: any }) {
      if (token) {
        session.user.id = token.id;
        session.user.name = token.name;
        session.user.email = token.email;
        session.user.role = token.role;
        session.user.image = token.image;
      }
      return session;
    },
  },
  secret: process.env.NEXTAUTH_SECRET,
  pages: {
    signIn: "/auth/signin",
  },
};

export default NextAuth(authOptions);