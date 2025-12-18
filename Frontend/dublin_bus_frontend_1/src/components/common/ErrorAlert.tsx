import React from 'react';
import { Alert } from 'react-bootstrap';

interface ErrorAlertProps {
  message: string;
  onDismiss?: () => void;
}

const ErrorAlert: React.FC<ErrorAlertProps> = ({ message, onDismiss }) => {
  return (
    <Alert variant="danger" dismissible={!!onDismiss} onClose={onDismiss}>
      {message}
    </Alert>
  );
};

export default ErrorAlert;
