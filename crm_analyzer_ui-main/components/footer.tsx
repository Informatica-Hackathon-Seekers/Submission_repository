"use client";

import Link from "next/link";
import { motion } from "framer-motion";
import { Twitter, Linkedin, Instagram, Github } from "lucide-react";

const Footer = () => {
  const socialLinks = [
    { icon: <Twitter size={18} />, href: "#", label: "Twitter" },
    { icon: <Linkedin size={18} />, href: "#", label: "LinkedIn" },
    { icon: <Instagram size={18} />, href: "#", label: "Instagram" },
    { icon: <Github size={18} />, href: "#", label: "GitHub" },
  ];

  const footerLinks = [
    { name: "About Us", href: "/about" },
    { name: "Contact", href: "/contact" },
    { name: "Privacy Policy", href: "/privacy" },
    { name: "Terms of Service", href: "/terms" },
  ];

  return (
    <footer className="border-t bg-background">
      <div className="container px-4 py-8 md:py-12 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 gap-8 md:grid-cols-3">
          {/* Brand Section */}
          <div className="space-y-4">
            <Link href="/" className="flex items-center space-x-2">
              <motion.div whileHover={{ rotate: 10 }}>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="h-6 w-6"
                >
                  <path d="m22 7-8.5 8.5-5-5L2 17" />
                  <path d="M16 7h6v6" />
                </svg>
              </motion.div>
              <span className="font-bold text-xl">Trendlytics</span>
            </Link>
            <p className="text-sm text-muted-foreground">
              Your gateway to the latest trends, stock analysis, and more. Unlock
              insights, elevate decisions.
            </p>
          </div>

          {/* Links Section */}
          <div className="space-y-4">
            <h3 className="text-sm font-medium">Links</h3>
            <ul className="space-y-2">
              {footerLinks.map((link) => (
                <li key={link.href}>
                  <Link href={link.href}>
                    <motion.span
                      className="text-sm text-muted-foreground hover:text-foreground"
                      whileHover={{ x: 5 }}
                      transition={{ type: "spring", stiffness: 400, damping: 10 }}
                    >
                      {link.name}
                    </motion.span>
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Social Links Section */}
          <div className="space-y-4">
            <h3 className="text-sm font-medium">Connect with us</h3>
            <div className="flex space-x-4">
              {socialLinks.map((social) => (
                <Link key={social.label} href={social.href}>
                  <motion.div
                    className="flex h-8 w-8 items-center justify-center rounded-full border hover:bg-muted"
                    whileHover={{ y: -3 }}
                    whileTap={{ scale: 0.95 }}
                    aria-label={social.label}
                  >
                    {social.icon}
                  </motion.div>
                </Link>
              ))}
            </div>
          </div>
        </div>

        {/* Copyright Section*/}
        <div className="mt-8 border-t pt-6">
          <p className="text-center text-xs text-muted-foreground">
            © {new Date().getFullYear()} Trendlytics. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
};

export default Footer;