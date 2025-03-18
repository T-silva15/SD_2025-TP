public class Wavy
{
	public static void Main(string[] args)
	{
		while (true)
		{
			Thread.Sleep(10000);
			var freq = GenerateTemp(108, 0);
			Console.WriteLine($"Frequency: {freq}Hz");
			Thread.Sleep(10000);
			var db = GenerateTemp(150, 0);
			Console.WriteLine($"Decibels: {db}dB");
			Thread.Sleep(10000);
			var wind = GenerateTemp(300, 0);
			Console.WriteLine($"Wind Speed: {wind}km/h");
			Thread.Sleep(30000);
			var temp = GenerateTemp(40, 0);

			Console.WriteLine($"Temperature: {temp}°C");
		}
	}

	public static float GenerateTemp(int max, int min)
	{		
		Random random = new Random();
		var x = (float)random.NextDouble() * (max - min) + min;
		return x;
	}


}
