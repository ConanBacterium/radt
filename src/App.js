import React from 'react';
import './App.css';

class App extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      error: null,
      isLoaded: false,
      items: []
    };
  }

  componentDidMount() {

    fetch("https://jsonplaceholder.typicode.com/posts?_limit=15")
      .then(res => res.json())
      .then(
        (results) => {
          this.setState({
            isLoaded: true,
            items: results
          });
        },
        // Note: it's important to handle errors here
        // instead of a catch() block so that we don't swallow
        // exceptions from actual bugs in components.
        (error) => {
          this.setState({
            isLoaded: true,
            error
          });
        }
      )
  }

  render() {
    
    const { error, isLoaded, items } = this.state;

    if (error) {
      return <div>Error: {error.message}</div>;
    } 
    else if (!isLoaded) {
      return <div>Loading...</div>;
    } 
    else {
      return (
        <ul>
          {items.map(item => (
            <li key={item.id}>
              {item.title} {item.body}
            </li>
          ))}
        </ul>
      );
    }
  }

}

export default App;
