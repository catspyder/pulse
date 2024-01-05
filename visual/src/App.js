
import './App.css';
import Card from './componetns/card/card';
import {React,useEffect,useState} from 'react';

function App() {
  const [data,setData] = useState([]);
  const updateData = (da) => {
   setData(da);
    
  }
  const url = "http://localhost:3000/api/data";
  useEffect(() => {
    fetch(url).then(res => res.json()).then(data => {
      
    })

  },[])
  return (
    <div className="App">
      <Card className="big-card"/>
    </div>
  );
}

export default App;
