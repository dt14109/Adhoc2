package rates;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Rates {

	public static void main(String[] args) {

		String header = "zipcode,rate";
		
		// read each zip from slcsp
		// Go to zips and find the matching zip, if it's in there more than once, answer is "" and this zip is done.
		// Otherwise, get zip info:
		//					zipcode,state,county_code,name,rate_area
		//					36749,AL,01001,Autauga,11
		
		//  Go to plans (only want the Silver ones) and search for each matching state/rate_area and read the rate into a List<Double> 					
		//					plan_id,state,metal_level,rate,rate_area
		//					74449NR9870320,GA,Silver,298.62,7					
		// 					
		// Read the List<Double> for the 2nd lowest
		// If there's not a 2nd lowest, answer is ""
		
		// Read the files and store them in streams for the most efficient processing 
		String slcspFile = "/adhoc_java/rates/src/com/rates/resources/slcsp.csv";
		String plansFile = "/adhoc_java/rates/src/com/rates/resources/plans.csv";
		String zipsFile = "/adhoc_java/rates/src/com/rates/resources/zips.csv";

		// try will automatically close the File
		try (Stream<String> slcspStream = Files.lines(Paths.get(slcspFile))) {
			try (Stream<String> plansStream = Files.lines(Paths.get(plansFile))) {
				try (Stream<String> zipsStream = Files.lines(Paths.get(zipsFile))) {

					// Initialize us with the header values
					List<String> results = Stream.of(header).collect(Collectors.toList());
					
					// read each zip from slcsp
					// Go to zips and find the zip, if it's in there > 1, answer is ""
					// else get zip info:
					//					zipcode,state,county_code,name,rate_area
					//					36749,AL,01001,Autauga,11
					
					// Go to plans (filter on Silver) and search for each matching state/rate_area and read the rate into a List<Double> 
					// with a key of rate.
					//					plan_id,state,metal_level,rate,rate_area
					//					74449NR9870320,GA,Silver,298.62,7					
					// 					
					// Read the List<Double> for the 2nd lowest					

					// Only deal with the Silver plans
					// The following produces a Map where State + Rate_Area is the key and Rate is the value
					Map<String, List<Double>> rates = plansStream.filter(e -> e.indexOf("Silver") > -1)
							.map(Rates::mapPlansToArray) // This produces the String[] of [rate, state + rate_area] 
							.collect(Collectors.groupingBy(e -> e[1],  // This produces our String (state + rate_area)
									Collectors.mapping(Rates::mapToRate, Collectors.toList()))); // This produces our Double (rate) 

					// This produces a map where zip code is the key and state + rate areas is the value (skip the header)
					Map<String, Set<String>> zips = zipsStream.skip(1)
							.map(Rates::mapZipsToArray) // This produces the String[] of [zip, state + rate_area] 
							.collect(Collectors.groupingBy(e -> e[0],  // This produces our String (zip)
									Collectors.mapping(f -> f[1], Collectors.toSet()))); // This produces our Integer (state + rate_area) 
					
					// Ignore the header and the ending ","
					// For each zip, get the state + rate areas
					slcspStream.skip(1).forEach(zip -> {
						// Get the state + rate areas for this zip
						Set<String> state_rateareas = zips.get(zip.substring(0, zip.length() - 1));
						
						// Make sure we have data and there is only one unique value (because zips in multiple State/Rate Areas are not valid)
						if(state_rateareas.size() > 1) {
							// If there is more than 1 unique, the answer is blank
							results.add(zip);
							return;
						}
						
						if(null != state_rateareas && 1 == state_rateareas.size()) {
							Set<Double> set = new TreeSet<Double>(); // TreeSet is sorted by default and does not have duplicates
							Double temp = 0.0;
							int count = 0;
							String buffer;
							
							// There's only one item but this is the easiest way
							for(String ra : state_rateareas) {
								List<Double> ld = rates.get(ra);
								if(null != ld) {
									set.addAll(ld);
								}
							}
														
							// At this point we have all the rates.  We need to find the 2nd lowest
							if(0 == set.size()) {
								results.add(zip);
							}
							else if(set.size() > 1) {
								// This set is sorted and we want the 2nd item.  But there's not a way 
								// to directly get the 2nd item out of the set. So we have to loop or iterate.
								for (Double d : set) {
									if(1 == count) {
										temp = d;
										break;
									}
									count++;
							    }
								buffer = String.format("%.2f", temp);
							 	results.add(zip + buffer);
							}
							else {
								// Zip is in more than 1 rate area, just add the zip
								results.add(zip);
							}
						}
						else {
							// Couldn't find us, just adding zip
							results.add(zip);
						}
					});
					
					// Done, print the results
					results.stream().forEach(System.out::println);
					
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
	
	/**
	 * Plan comes in as a String in the format of: plan_id, state, metal, rate, rate_area 
	 * and we map it to a String[] with only<rate, state + rate_area>
	 * @param String plan
	 * @return String[]  <rate, state + rate_area>
	 */
	private static String[] mapPlansToArray(String plan) {
		String[] array = plan.split(",");
		String[] rates = {array[3], array[1] + array[4]};
		return rates;
	}

	/**
	 * Plan comes in as a String[] of: rate, state + rate_area and we return the rate
	 * @param String[] plan
	 * @return Double rate
	 */
	private static Double mapToRate(String[] plan) {
		return Double.parseDouble(plan[0]);
	}

	/**
	 * Zips comes in as a String in the format of: zip_code, state, county_code, name, rate_area 
	 * and we map it to a String[] with only<zip_code, state + rate_area>
	 * @param String plan
	 * @return String[]  <rate, rate_area>
	 */
	private static String[] mapZipsToArray(String zips) {
		String[] array = zips.split(",");
		String[] rates = {array[0], array[1] + array[4]};
		return rates;
	}
}
