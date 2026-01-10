import "./globals.css";
import Nav from "@/components/Nav";

export const metadata = { title: "Market Screener" };

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">
        <div className="max-w-7xl mx-auto px-4 py-4">
          <Nav />
          <div className="mt-4">{children}</div>
        </div>
      </body>
    </html>
  );
}
