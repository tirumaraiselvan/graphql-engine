Action handlers
===============


.. contents:: Table of contents
  :backlinks: none
  :depth: 1
  :local:

Actions need to be backed by custom business logic. This business logic can be defined in different types of handlers.


HTTP handler
------------

Suited for complex logic.
Scale
Business logic separated from data store.
Show session variables

.. code-block:: python

   def place_order(payload):
       input_args = payload['input']
       session_variables = payload['session_variables']
       # code to validate this mutation and insert into the database
       order_id = validate_and_insert_order(input_args, session_variables)
       return {"order_id": order_id}


Postgres functions
------------------

Ideal for simple and small validations


PLV8
----

Similar to postgres functions but in nodejs.


Managing and deploying action handlers
--------------------------------------

HTTP handlers in serverless functions, micoservice APIs etc 

PG functions as migrations, etc
