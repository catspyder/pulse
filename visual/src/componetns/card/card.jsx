import React from 'react';
import './card.css';

const Card = (props) => {
    console.log(props.children);
    return (
        <div className="frosted-glass">
            {props.children}
        </div>
    );
};

export default Card;
