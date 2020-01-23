FROM node

WORKDIR /producer

COPY package*.json /producer/

RUN npm install

COPY . /producer/

EXPOSE 4000
CMD [ "npm", "start"]
