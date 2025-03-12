import { dbConnect } from "@/lib/dbConnect";
import UserModel from "@/model/User";
import { NextResponse } from "next/server";

export async function PUT(request: Request) {
  try {
    // Parse the request body
    const { email, topStocks } = await request.json();

    // Validate required fields
    if (!email) {
      return NextResponse.json(
        { message: "Email is required to update the user." },
        { status: 400 }
      );
    }

    if (!topStocks || !Array.isArray(topStocks)) {
      return NextResponse.json(
        { message: "topStocks must be an array." },
        { status: 400 }
      );
    }

    // Validate topStocks length
    if (topStocks.length > 5) {
      return NextResponse.json(
        { message: "Top stocks cannot exceed 5." },
        { status: 400 }
      );
    }

    await dbConnect();

    // Find the user by email
    const user = await UserModel.findOne({ email });
    if (!user) {
      return NextResponse.json(
        { message: "User not found." },
        { status: 404 }
      );
    }

    // Update the topStocks field
    user.topStocks = topStocks;

    // Save the updated user
    await user.save();

    return NextResponse.json(
      { message: "Top stocks updated successfully.", user },
      { status: 200 }
    );
  } catch (error) {
    console.error("Error updating topStocks:", error);
    return NextResponse.json(
      { message: "Something went wrong." },
      { status: 500 }
    );
  }
}