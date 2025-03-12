import mongoose, { Document, Schema } from "mongoose";

interface IUser extends Document {
  name?: string;
  email: string;
  username: string;
  password?: string; // Nullable for Google sign-in
  role: string;
  userType: string;
  tier: "Starter" | "Pro" | "Elite";
  topStocks: string[];
  verifyCode?: string;
  verifyCodeExpiry?: Date;
  isVerified: boolean;
  provider: "google" | "credentials";
  image?: string;
  createdAt: Date;
  updatedAt: Date;
}

const userSchema = new Schema<IUser>(
  {
    name: { type: String },
    email: { type: String, required: true, unique: true },
    username: { type: String, unique: true, sparse: true },
    password: { type: String },
    role: { type: String, default: "user", enum: ["admin", "user"] },
    userType: { type: String, enum: ["professional", "personal"], default: "personal" },
    tier: { type: String, enum: ["Starter", "Pro", "Elite"], default: "Starter" },
    topStocks: { type: [String], validate: [(val: string[]) => val.length <= 5, "Top stocks cannot exceed 5"] },
    verifyCode: { type: String },
    verifyCodeExpiry: { type: Date },
    isVerified: { type: Boolean, default: false },
    provider: { type: String, enum: ["google", "credentials"], required: true },
    image: { type: String },
  },
  {
    timestamps: true,
  }
);

// Check if the model already exists to avoid recompiling it
const UserModel = mongoose.models.User || mongoose.model<IUser>("User", userSchema);

export default UserModel;