import { dbConnect } from "@/lib/dbConnect";
import UserModel from "@/model/User";
import bcrypt from "bcryptjs";
import { NextResponse } from "next/server";

export async function POST(request: Request) {
  try {
    // Parse the request body
    const { firstName, lastName, email, password, topStocks } = await request.json();

    console.log("firstName : ", firstName);
    console.log("topStocks : ", topStocks);

    await dbConnect();

    // Check if the user already exists
    const existingUser = await UserModel.findOne({ email });
    if (existingUser) {
      return NextResponse.json(
        { message: "User already exists." },
        { status: 400 }
      );
    }

    // Hash the password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Create a new user
    const newUser = new UserModel({
      name: `${firstName} ${lastName}`,
      email,
      password: hashedPassword,
      tier: "Starter", // Default tier
      provider: "credentials",
      topStocks: topStocks || [], // Use topStocks from request or default to empty array
      username: `${firstName} ${lastName}`.split("@")[0], // Assign a username based on email
    });

    await newUser.save();

    return NextResponse.json(
      { message: "User created successfully." },
      { status: 201 }
    );
  } catch (error) {
    console.error("Error during sign-up:", error);
    return NextResponse.json(
      { message: "Something went wrong." },
      { status: 500 }
    );
  }
}