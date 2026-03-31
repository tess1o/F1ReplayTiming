import AuthGate from "@/components/AuthGate";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return <AuthGate>{children}</AuthGate>;
}
