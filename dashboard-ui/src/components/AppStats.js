import React, { useEffect, useState } from 'react';
import '../App.css';

export default function AppStats() {
  const [isLoaded, setIsLoaded] = useState(false);
  const [stats, setStats] = useState({});
  const [error, setError] = useState(null);

  const getStats = () => {
    fetch(`http://sean-kafka.eastus2.cloudapp.azure.com/stats`)
      .then((res) => res.json())
      .then(
        (result) => {
          console.log('Received Stats');
          setStats(result);
          setIsLoaded(true);
        },
        (error) => {
          setError(error);
          setIsLoaded(true);
        }
      );
  };
  useEffect(() => {
    const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
    return () => clearInterval(interval);
  }, [getStats]);

  if (error) {
    return <div className={'error'}>Error found when fetching from API</div>;
  } else if (isLoaded === false) {
    return <div>Loading...</div>;
  } else if (isLoaded === true) {
    return (
      <div>
        <h1>Latest Stats</h1>
        <table className={'StatsTable'}>
          <tbody>
            <tr>
              <th>Delviery</th>
              <th>Order</th>
            </tr>
            <tr>
              <td># Delivery Orders: {stats['num_delivery_orders']}</td>
              <td># Pickup Orders: {stats['num_pickup_orders']}</td>
            </tr>
            <tr>
              <td colspan='2'>
                Max Total Delivery: {stats['max_total_delivery']}
              </td>
            </tr>
            <tr>
              <td colspan='2'>
                Drivers:{' '}
                {stats['num_drivers'] &&
                  stats['num_drivers'].map((driver) => driver)}
              </td>
            </tr>
          </tbody>
        </table>
        <h3>Last Updated: {stats['last_updated']}</h3>
      </div>
    );
  }
}
