import { useEffect, useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  AreaChart,
  Area,
  Cell,
  LineChart,
  Line
} from "recharts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp, RefreshCw } from "lucide-react";

// Define interfaces for data
interface RegionStat { region: string; count: number; }
interface PriceStat { region: string; min_price: number; max_price: number; avg_price: number; }
interface TransactionStat { transaction_type: string; count: number; }
interface PropertyTypeStat { type: string; count: number; }
interface TopArea { city: string; count: number; }
interface TrendStat { date: string; count: number; }
interface PriceM2Stat { region: string; avg_m2: number; }

interface EDAData {
  region_stats: RegionStat[];
  price_stats: PriceStat[];
  transaction_stats: TransactionStat[];
  property_type_stats: PropertyTypeStat[];
  top_areas: TopArea[];
  trend_stats: TrendStat[];
  price_m2_stats: PriceM2Stat[];
}

const COLORS = [
  '#FFB3BA', // Pastel Red
  '#FFDFBA', // Pastel Orange
  '#FFFFBA', // Pastel Yellow
  '#BAFFC9', // Pastel Green
  '#BAE1FF', // Pastel Blue
  '#E2F0CB', // Pastel Lime
  '#F1CBFF', // Pastel Purple
  '#D7FFD9', // Pastel Mint
  '#FFC4E1'  // Pastel Pink
];

export function EDADashboard() {
  const [data, setData] = useState<EDAData | null>(null);
  const [loading, setLoading] = useState(true);
  const [isOpen, setIsOpen] = useState(true);

  const fetchData = async () => {
    setLoading(true);
    try {
      // Use relative path to leverage Vite proxy in development
      // Add credentials: "include" to ensure session cookie is sent with request
      const response = await fetch("/api/eda/", { credentials: "include" }); 
      
      if (response.ok) {
        const result = await response.json();
        setData(result);
      } else {
          console.error("Failed to fetch EDA data: " + response.statusText);
      }
    } catch (error) {
      console.error("Failed to fetch EDA data", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    // Poll every 30 seconds for updates during reprocessing
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  if (loading && !data) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Exploratory Data Analysis</CardTitle>
          <CardDescription>Loading analytics data...</CardDescription>
        </CardHeader>
        <CardContent className="h-[300px] flex items-center justify-center">
           <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
        </CardContent>
      </Card>
    );
  }

  if (!data) {
    return (
      <Card className="w-full border-destructive/50">
        <CardHeader>
          <CardTitle className="text-destructive">Error Loading Analytics</CardTitle>
          <CardDescription>
             Unable to fetch data. Please ensure the backend server is running.
             <Button variant="outline" size="sm" onClick={fetchData} className="ml-4">Retry</Button>
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  // Pre-process data for consistent rendering
  const topPrices = [...data.price_stats]
      .sort((a, b) => b.avg_price - a.avg_price)
      .slice(0, 15)
      .map(item => ({...item, avg_price: Math.round(item.avg_price / 1000)}));

  const topPricesM2 = data.price_m2_stats.map(item => ({...item, avg_m2: Math.round(item.avg_m2 / 1000)}));
  
  const topRegions = [...data.region_stats].sort((a, b) => b.count - a.count).slice(0, 15);

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen} className="space-y-4">
        <div className="flex items-center justify-between">
            <h2 className="text-2xl font-bold tracking-tight">Exploratory Data Analysis</h2>
            <div className="flex gap-2">
                 <Button variant="outline" size="sm" onClick={fetchData} disabled={loading}>
                    <RefreshCw className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                    Refresh
                 </Button>
                 <CollapsibleTrigger asChild>
                    <Button variant="ghost" size="sm">
                        {isOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                    </Button>
                 </CollapsibleTrigger>
            </div>
        </div>

      <CollapsibleContent className="space-y-4">
        {/* Top Row: Regional Distribution & Transaction Types */}
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
          <Card className="col-span-4">
            <CardHeader>
              <CardTitle>Regional Distribution</CardTitle>
              <CardDescription>Number of listings by region (Governorate)</CardDescription>
            </CardHeader>
            <CardContent className="pl-2">
              <ResponsiveContainer width="100%" height={350}>
                <BarChart data={topRegions}>
                  <XAxis dataKey="region" stroke="#888888" fontSize={12} tickLine={false} axisLine={false} />
                  <YAxis stroke="#888888" fontSize={12} tickLine={false} axisLine={false} />
                  <Tooltip 
                    contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.9)', borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                  />
                  <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                    {topRegions.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          <Card className="col-span-3">
             <CardHeader>
                <CardTitle>Transaction Types</CardTitle>
                <CardDescription>Distribution of Sales vs Rent</CardDescription>
             </CardHeader>
             <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                        <Pie
                            data={data.transaction_stats}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={80}
                            paddingAngle={5}
                            dataKey="count"
                            nameKey="transaction_type"
                        >
                            {data.transaction_stats.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip />
                        <Legend verticalAlign="bottom" height={36}/>
                    </PieChart>
                </ResponsiveContainer>
             </CardContent>
          </Card>
        </div>
        
        {/* Second Row: Property Types (New) & Price Analysis */}
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
           <Card className="col-span-3">
             <CardHeader>
                <CardTitle>Property Types</CardTitle>
                <CardDescription>Apartment vs Villa vs Land, etc.</CardDescription>
             </CardHeader>
             <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                        <Pie
                            data={data.property_type_stats}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="count"
                            nameKey="type"
                        >
                            {data.property_type_stats.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[(index + 2) % COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip />
                    </PieChart>
                </ResponsiveContainer>
             </CardContent>
          </Card>

            <Card className="col-span-4">
                <CardHeader>
                    <CardTitle>Price Analysis by Region</CardTitle>
                    <CardDescription>Top 15 Most Expensive Regions (Avg Price in Thousands TND)</CardDescription>
                </CardHeader>
                <CardContent>
                    <ResponsiveContainer width="100%" height={300}>
                        <BarChart 
                            data={topPrices}
                            layout="vertical"
                            margin={{ left: 20 }}
                        >
                            <XAxis type="number" fontSize={12} tickLine={false} axisLine={false} />
                            <YAxis dataKey="region" type="category" width={100} fontSize={12} tickLine={false} axisLine={false} />
                            <Tooltip formatter={(value: number) => new Intl.NumberFormat('en-US', { style: 'decimal' }).format(value) + ' TND'} />
                            <Bar dataKey="avg_price" name="Avg Price" radius={[0, 4, 4, 0]}>
                                {topPrices.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={COLORS[(index + 3) % COLORS.length]} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </CardContent>
            </Card>
        </div>

        {/* Third Row: Price/m2 */}
        <div className="grid gap-4 md:grid-cols-1">
            <Card>
                <CardHeader>
                    <CardTitle>Avg Price per m²</CardTitle>
                    <CardDescription>By Region (Thousands TND/m²)</CardDescription>
                </CardHeader>
                <CardContent>
                    <ResponsiveContainer width="100%" height={300}>
                        <BarChart 
                            data={topPricesM2}
                            layout="vertical"
                        >
                            <XAxis type="number" fontSize={12} tickLine={false} axisLine={false} />
                            <YAxis dataKey="region" type="category" width={100} fontSize={12} tickLine={false} axisLine={false} />
                            <Tooltip formatter={(value: number) => new Intl.NumberFormat('en-US', { style: 'decimal' }).format(value) + ' TND/m²'} />
                            <Bar dataKey="avg_m2" radius={[0, 4, 4, 0]}>
                                {topPricesM2.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={COLORS[(index + 4) % COLORS.length]} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </CardContent>
            </Card>
        </div>
        
        {/* Fourth Row: Trends and Top Areas */}
        <div className="grid gap-4 md:grid-cols-2">
             <Card>
                <CardHeader>
                    <CardTitle>Listing Frequency Over Time</CardTitle>
                    <CardDescription>New listings added per day</CardDescription>
                </CardHeader>
                <CardContent>
                    <ResponsiveContainer width="100%" height={300}>
                        <AreaChart data={data.trend_stats}>
                            <defs>
                                <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#BAE1FF" stopOpacity={0.8}/>
                                    <stop offset="95%" stopColor="#BAE1FF" stopOpacity={0}/>
                                </linearGradient>
                            </defs>
                            <XAxis dataKey="date" fontSize={12} tickLine={false} axisLine={false} />
                            <YAxis fontSize={12} tickLine={false} axisLine={false} />
                            <Tooltip />
                            <Area type="monotone" dataKey="count" stroke="#BAE1FF" fillOpacity={1} fill="url(#colorCount)" />
                        </AreaChart>
                    </ResponsiveContainer>
                </CardContent>
             </Card>

             <Card>
                <CardHeader>
                    <CardTitle>Top 10 Most Active Areas</CardTitle>
                    <CardDescription>By City/Municipality</CardDescription>
                </CardHeader>
                <CardContent>
                    <ResponsiveContainer width="100%" height={300}>
                        <BarChart data={data.top_areas} layout="vertical">
                            <XAxis type="number" fontSize={12} tickLine={false} axisLine={false} />
                            <YAxis dataKey="city" type="category" width={120} fontSize={12} tickLine={false} axisLine={false} />
                            <Tooltip />
                            <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                                {data.top_areas.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={COLORS[(index + 5) % COLORS.length]} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </CardContent>
             </Card>
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}
